# Known Issues

A severity-ranked index of open defects on the `ZeroCopyResponses` branch. Each entry names
the symptom, the cause, and a `file:line`.

**How these were established.** Almost everything here comes from reading the source. Entries
that were later confirmed by reproduction say so on their resolved page, and item 14 was the last
one on this page to carry that note before it moved
([Resolved #14](resolved/empty-rotated-logs.md#evidence)). Item 75 is the exception and says so in
its own **Evidence** note: it was found by reading a committed capture artifact against the page
generated from it, which is a third way of finding a defect that this list had no instance of until
`f22-row-size` was taken. Item 76 was found the same way and is now
[resolved](resolved/stage-join.md), where its note records the run that reproduced it.

**Line numbers drift, and they had.** Every citation on this page was re-resolved against the tree
in August 2026 ([Review](review-2026-08.md)) and most of them had moved — item 16's whole table by
a hundred lines or more. They are correct as of that sweep and will rot again; the symbol name
beside each one is what a reader should grep for, and the line is a shortcut. That review also
found that two entries had been quietly overtaken by the code — item 17 is now mostly fixed, and
item 25 is fully [resolved](resolved/claude-md-drift.md).

Reading is enough to find a defect and not always enough to characterise it. Item 13 was filed
from a reading that called its panic reachable; trying to reproduce it showed the panic was
latent and that the line's live cost was a different one
([Resolved #13](resolved/eviction-log-underflow.md#evidence)). An entry here is a claim about the
source, not yet a claim about a running server.

Performance findings are catalogued separately in [Optimizations](optimizations.md), and what the
test suite does and does not reach is in [Test Coverage](test-coverage.md).

Defects that have been fixed move to [Resolved Issues](resolved-issues.md), one page each,
carrying the reasoning and the invariants the fix depends on. Item numbers are shared between
the two pages and never reused, so a number appears on exactly one of them — which is why this
list starts at ~~15~~ ~~16~~ 19 and skips 25, 26, 27, 30, 31, 32, 33, 34, 36, 38, 39, 43, 44, 45, 46, 48, 51, 56, 57, 58, 61, 67, 68, 74,
76, 78, 79, 80, 82, 83, 84, 85, 86, 88, 89, 90, 92, 93, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 114, 115, 116, 120, 121, 122, 123 and 124, and
why ~~item 91~~ ~~item 97~~ ~~item 100~~ ~~item 103~~ ~~item 107~~ ~~item 109~~ ~~item 110~~ ~~item 112~~ ~~item 113~~ ~~item 119~~ ~~item 124~~ ~~item 125~~ ~~item 119 is the newest entry here again~~ ~~and the newest number~~ ~~with 114 the newest number, on the resolved page~~ ~~and the newest number~~ ~~with 121 the newest number, on the resolved page~~ ~~with 124 the newest number, on the resolved page~~ ~~and the newest number~~ ~~with 125 the newest number, on the resolved page~~ ~~with 126 the newest number, on the resolved page~~ ~~with 127 the newest number, on the resolved page~~ ~~item 129 is the newest entry and the newest number~~ ~~item 131 is the newest entry and the newest number~~ ~~item 132 is the newest entry and the newest number~~ ~~item 180 is the newest entry, with 182 the newest number, on the resolved page,~~ ~~item 142 is the newest open entry, with 187 the newest number, on the resolved page,~~ ~~item 196 is the newest entry and the newest number, with 191, 192, 194 and 195 filed and fixed in one change on the resolved page,~~ ~~item 142 is the newest open entry, with 197 the newest number, on the resolved page beside 193 and 196 (all three from round 16 of the cluster testing, 197 filed and fixed in one change),~~ ~~item 198 is the newest entry and the newest number (filed by [F66](../features/dataset-benchmarks.md), which found it),~~ ~~item 198 is the newest entry, with 200 the newest number, on the resolved page beside 199 (both filed and fixed in one change, from a user's `shoaladm bench` run whose ops/s by kind chart stayed empty),~~ ~~item 198 is the newest entry, with 201 the newest number, on the resolved page beside 199 and 200 (all three filed and fixed in one change each, from a user's `shoaladm bench` runs)~~ ~~item 206 is the newest entry and the newest number~~ ~~item 207 is the newest number, filed by [Resolved #92, #198](resolved/composite-partition-key.md) beside item 42 that it belongs with~~ ~~item 209 is the newest number~~ ~~item 211 is the newest number~~ ~~item 212 is the newest number~~ ~~item 213 is the newest number~~ ~~item 218 is the newest number~~ ~~item 219 is the newest number~~ ~~item 222 is the newest number~~ ~~item 223 is the newest number~~ ~~item 224 is the newest number~~ ~~item 225 is the newest number~~ ~~item 226 is the newest number~~ ~~item 227 is the newest number~~ ~~item 229 is the newest number~~ ~~item 230 is the newest number~~ ~~item 231 is the newest number, on the resolved page:~~ ~~item 236 is the newest number~~ ~~item 237 is the newest number~~ ~~item 238 is the newest number~~ ~~item 240 is the newest number, on the resolved page with 239~~ ~~item 248 is the newest number, filed by~~ ~~item 249 is the newest number, filed by~~ ~~item 250 is the newest number, filed by~~ ~~item 253 is the newest number, on the resolved page:~~ ~~item 254 is the newest number, filed by~~ ~~item 259 is the newest number, on the resolved page with 258:~~ ~~item 272 is the newest number:~~ ~~item 273 is the newest number, on the resolved page with 260:~~ ~~item 275 is the newest number:~~ ~~item 277 is the newest number, on the resolved page with 276:~~ ~~item 279 is the newest number:~~ ~~item 280 is the newest number:~~ ~~item 282 is the newest number:~~ ~~item 325 is the newest number:~~ ~~item 326 is the newest number:~~ ~~327 is the newest number, on the resolved page:~~ ~~328 is the newest number, on the resolved page:~~ ~~329 is the newest number:~~ ~~330 is the newest number, on the resolved page:~~ ~~331 is the newest number, on the resolved page:~~ ~~333 is the newest number:~~ ~~336 is the newest number:~~ ~~337 is the newest number, on the resolved page:~~ ~~338 is the newest number:~~ ~~340 is the newest number, filed while item 339 was fixed:~~ ~~342 is the newest number, on the resolved page:~~ 343 is the newest number, on the resolved page: a get failed on a partition an archive pass only moved under it, found while F120 was planned and never here, since it was fixed in the change after F120's ([Resolved #343](resolved/moved-partition-read-again.md)); 342, on the resolved page: a removed archive was kept open again by a read that opened it while it was removed ([Resolved #342](resolved/removed-archive-cached-again.md)); 341, on the resolved page: a scrub's canonical cut read an archive a pass removed meanwhile with no checksum ([Resolved #341](resolved/held-handle-reads-verified.md)), both found and fixed while F120 was built; 340, filed while item 339 was fixed: an archive file the map no longer knows is never deleted; 339, on the resolved page: an archive written since the map's last commit was unknown after a restart, found while F120's design was reviewed ([Resolved #339](resolved/archives-known-after-restart.md)); 338, filed by [X16](../future-work/scans/walk-beside-the-foreground.md#what-was-found-on-the-way)'s first quick pass: a node whose resident set at rest is past its node memory evicts its rows at every check and never gets under it; 337, on the resolved page: a cluster's first initialization was asked once, and a control election under it failed the start, from the third workspace run after the confirmation loops ([Resolved #337](resolved/initialization-rides-out-an-election.md)); 336, on the resolved page: a restore's group failed for good when a second drive judged it non-empty by the first drive's install, from the second workspace run after the confirmation loops, fixed under wire 17 ([Resolved #336](resolved/restore-drives-fenced.md)); 335, on the resolved page: the scrub cursor row compared one copy's cursor with another's, the test's own ([Resolved #335](resolved/scrub-cursor-row-reads-one-copy.md)); 334, on the resolved page: the control core row excluded the control core a plan made alone named, item 328's fallout ([Resolved #334](resolved/control-core-row-takes-its-own-allocation.md)); 333, on the resolved page: a device removed as lost was still asked as a holder, and the moves off it crawled, found planning item 250's fix and fixed beside it ([Resolved #333](resolved/removed-device-serves-nothing.md)), with 250's first half ([Resolved #250, the rest](resolved/empty-groups-move-without-a-slot.md)); 332, on the resolved page: `truncated_bytes_never_return` raced its writer with a 400 ms sleep, the test's own ([Resolved #332](resolved/truncation-row-waits-for-its-writer.md)); 331, on the resolved page: a plan's move that lost every try to the topology version was failed, found in the flaky-test plan's evidence loop and filed and fixed in one change ([Resolved #331](resolved/plan-move-lost-to-the-version.md)); 330, on the resolved page: a light scrub that could not ask a chunk's owner for now moved past its placement group, filed and fixed in one change ([Resolved #330](resolved/scrub-refused-for-now.md)); 329: a group of two voters elected no leader for a minute under load, filed from the flaky-test plan's confirmation; 328, on the resolved page: the fixture's clusters started beside each other were all given the same cores, filed and fixed in one change ([Resolved #328](resolved/clusters-beside-each-other.md)); 327, on the resolved page: a group's restore finished by a drive that never installed it reported nothing restored, item 142's most frequent row, filed and fixed in one change ([Resolved #327](resolved/restore-figures-kept.md)); 326: every get one node coordinated on the lab's disk leg stopped on a read of its own disk, with nothing logged, filed from F101's third run; 325, on the resolved page: a node just rolled onto a build answered a query of a table not yet committed `StaleTopology`, an Outcome failure of W4's workspace run filed at once; ~~item 324 is the newest number,~~ 324, on the resolved page: a repair or a backup was forgotten unfinished once 64 newer ones were kept, found reading the control state while fixing 323, and fixed in the same window at the user's word ([Resolved #324](resolved/unfinished-records-kept.md)); 323, on the resolved page: a backup's group queued behind a move or a repair of its set was never released, found reading the backup's lifecycle for item 322 and filed and fixed in one change ([Resolved #323](resolved/queued-backups-released.md)); ~~item 322 is the newest number:~~ 322, on the resolved page: a group's backup fails for good on a transient refusal, filed from item 315's loop, its archives' retry fixed ([Resolved #322, in part](resolved/archive-refusals-retried.md)) ~~and the rest open~~ and the rest fixed: a record of every group's outcome beside the files, which a restore refuses a backup without, and a table group tried again ([Resolved #322, the rest](resolved/backup-outcome-record.md)); ~~item 321 is the newest number,~~ 321, on the resolved page: `a_cancel_cuts_a_stream_being_written` found a cancelled stream written to its end, which was the row's own race, its client reading past its cancel at memory speed ([Resolved #321](resolved/cancel-row-waits-for-its-cancel.md)); ~~item 320 is the newest number,~~ 320, on the resolved page: the lab's flush counter (`stores-ab-flushes.py`) read one NVMe by name, which europa's reboot gave its other drive, filed and fixed in one change, every whole NVMe now reported by its name ([Resolved #320](resolved/flush-script-device-names.md)); ~~item 319 is the newest number,~~ 319, on the resolved page: a node whose peer refused its connections logged two WARN lines at every redial of every link, about nine a second a link, found in europa's journal, which a node left running with its peers gone filled on 2026-10-24; reproduced by a test and fixed, a run of failed dials warned of once and then every ten seconds, and a dial that never connected no longer logged as a link that went down ([Resolved #319](resolved/redial-warnings.md)); 318, on the resolved page: a query parked on a partition its paged archive map could not rule out, which the read finds does not exist, logged a WARN that the partition could not be read, 63,874 times on the comparison's loop leg, where nothing failed; reproduced by a test and fixed, a release with no error logged at DEBUG ([Resolved #318](resolved/absent-partition-release.md)); ~~item 317 is the newest number:~~ 317, on the resolved page since a test reproduced it and the client kept a connection's write half while it owes answers ([Resolved #317](resolved/owed-connection-kept.md)): a client's pooled connection past its pool's lifetime is closed with answers owed on it, so each connection costs an operation a retry every half hour, found reading the client and bb8 for how 316's get failed; 316: a get's request on an object lane went unanswered and its coordinator waited with no bound until its run ended, found in the fourth SSD capture of the comparison's second run by F108's figures, and again on its disk leg, where it was answered a minute or more late while the next arm's gets crossed the same links; ~~item 315 is the newest number:~~ 315: a restore done under load left an object it backed up unreadable to a strong get, and `a_restore_judges_each_table_by_its_own_identity` checks its groups' outcomes but not its archives', found by window three's workspace run; 314: `copies_on_disks_are_read_by_queue` found node zero's disk serving all sixteen reads it should have spread, found by the same run; ~~item 313 is the newest number, on the resolved page:~~ 313, on the resolved page: a slice's apply and its chain walked a chunk's records by calling themselves once for each, so a chain a disk's batched applies left a few hundred deep overflowed the object executor's stack and ended the node; found by F110's object suite on disks, reproduced on one slice, and filed and fixed in one change ([Resolved #313](resolved/record-chains-without-recursion.md)); ~~item 312 is the newest number:~~ 312: eight small writers on one stripe are answered `Busy` on disks about half the time, found by F109's rotational pass and as often at the commit before it; ~~item 311 is the newest number:~~ 311: a whole chunk's stream whose sender stops leaves its task waiting and its file held, found reading the object lane for item 305's silent waits; 310, on the resolved page: no peer lane and no client connection probed its peer, so one whose peer vanished while it owed nothing was never noticed, filed and fixed with keepalive at both ends ([Resolved #310](resolved/peer-connections-keepalive.md)); ~~item 309 is the newest number,~~ 309, on the resolved page: a connection's reader waited with no bound for a piped put's task to take its bytes, so one put whose task took none stopped every operation on its pooled connection, found reading item 305's server half and filed and fixed in one change ([Resolved #309](resolved/a-full-feed-leaves-its-connection-read.md)); ~~item 308 is the newest number,~~ 308, on the resolved page: a client's object attempt waited with no bound for its frames to be written and its answer, its retry budget running only between attempts, found reading item 305's client half and filed and fixed in one change ([Resolved #308](resolved/an-object-attempt-gives-up.md)); ~~item 307 is the newest number,~~ 307, on the resolved page: a served object lane whose writer stopped at an answer it could not frame or write went on reading, its answers dropped and its asker's requests waiting with no bound, found reading item 305's put path and filed and fixed in one change ([Resolved #307](resolved/a-lane-whose-writer-stops-ends.md)); ~~item 306 is the newest number,~~ 306, on the resolved page: an unattended upgrade's needrestart restarted a lab run mid-capture, its bench killed with no trap, and the run started again discarded the store volumes under the cluster that bench left and wrote an empty capture over a finished one; filed and fixed in one change ([Resolved #306](resolved/lab-run-restarted-over-its-cluster.md)); ~~item 305 is the newest number:~~ 305: a capture's 1 MiB puts across hosts stalled whole on item 304's first fix, and its check could not read 10 objects, nothing logged; not reproduced; ~~item 304 is the newest number: since F106 a strong table read across hosts beside gets that fill a link has a tail of about 110 ms where it was about 17, bisected on the lab to the glommio fork's vectored write, its cause not yet established;~~ ~~item 304 is the newest number, on the resolved page:~~ 304, on the resolved page: since F106's vectored write a frame is one segment, and one a full link dropped waited out the kernel's 200 ms loss probe, so a table's strong reads across hosts beside gets filling a link had a tail of about 110 ms where it was about 17; bisected on the lab, its cause tried there and fixed by a minimum RTO on the replication and control lanes and on a client's connection until it carries an object ([Resolved #304](resolved/lone-frame-loss-probe.md)); ~~item 303 is the newest number:~~ 303, on the resolved page: a dropped client's pool went on dialling its server and logged each attempt at ERROR, seen in the logs of lab 2's table path rounds and in F85's committed ones, reproduced through a counting forwarder and fixed in wave three ([Resolved #303](resolved/dropped-client-dials-nothing.md)); ~~item 302 is the newest number:~~ 302: `lease_reads_send_no_round_and_see_every_acknowledged_write` counts the groups' heartbeats against its bound on rounds, failed under load in F102's and F107's runs and filed at its second; ~~item 301 is the newest number, on the resolved page:~~ 301, on the resolved page: the bench's settle for Shoal was a ten second sleep, so the reclamation of a put arm's thousands of stripes ran through the arm after it, found in [F103](../features/plaintext-peer-lanes.md)'s run on the lab beside what O120's had seen and fixed before the next ([Resolved #301](resolved/a-settle-waits-for-reclamation.md)); ~~item 300 is the newest number:~~ 300: the inventory wizard builds an edited inventory's `object_timings` empty, found by reading the form while [F103](../features/plaintext-peer-lanes.md) carried its key through it; ~~item 299 is the newest number, on the resolved page:~~ 299, on the resolved page: the object executors' gate handed back a rebuild's, a move's and a scrub's request without its class, so recovery's reads used [F102](../features/read-unit-cache.md)'s cache and a scrub's listing ignored its device's ceiling; found by reading the served path while planning O104, filed and fixed in one change ([Resolved #299](resolved/a-gate-keeps-a-requests-class.md)); ~~item 297 is the newest number:~~ ~~item 298 is the newest number:~~ 298: F102's workspace run filed it, a row's filler writes shed by its group's queue under load, at the second run they were; F98's workspace run filed 296, a restore's table group left at `Installing` under load with its bucket archives waiting behind it, and 297, a restore that fails a group on a verifying scrub's transient, which [Tests That Fail Under Load](flaky-tests.md) records beside the rows that met them; ~~item 295 is the newest number:~~ 284 to 295 were filed when [Tests That Fail Under Load](flaky-tests.md) was begun, from its record of every fixture row that failed in a loaded run and passed when run again since 2026-10-11: five assert what the system answered or holds (284 to 286, 288 and 289), and seven are a deadline, a setup step or the fixture's own race (287 and 290 to 295); ~~item 283 is the newest number, on the resolved page:~~ 283, on the resolved page: [F96](../features/chunk-file-classes.md#on-the-lab)'s disk leg found a holder's decide discarding a committed small write that later writes were staged over, so a disk whose applies lagged behind its folds left its positions stuck and its stripes unreadable, filed and fixed in one change ([Resolved #283](resolved/chained-records-decided-together.md)); 282: [F96](../features/chunk-file-classes.md)'s workspace run and its reruns found `an_erasure_coded_move_decodes_what_its_old_holder_lost` missing its deadline under load, and passing alone; 281: the same runs found a placement group's move given up when its arrival's node is called down for a moment under load, which `an_operator_fails_a_device_and_its_groups_move_off_it` counts and which F95's run had met too; 280: F95's workspace run found `removed_device_id_is_never_accepted_again` counting a put's holders before a slow one is rebuilt, which failed it once alone; 279: shoal-bench's runner half stopped building without the engine before F94 found it; 278: F94's lab tries found an inventory whose pool the engine refuses deployed all the same, its nodes restarting until the bench gave up; 277 is on the resolved page with 276: [F94](../features/bench-object-endpoints.md) found an object preload given as a count putting half of each description's objects, and a put past a description's last object putting an earlier one again with no run saying so, and filed and fixed both in one change ([Resolved #276, 277](resolved/object-preload-and-wrap.md)); 274 and 275 are two fixture rows that failed twice under the workspace run of the fixes before them and passed alone; 273 is on the resolved page with 260: reproducing 260 found a merge tried again run after its table's later segments, which wrote its older rows over theirs, filed and fixed with it ([Resolved #260, 273](resolved/checkpoint-in-segment-order.md)), and 261 and 262 are on the resolved page too ([Resolved #261](resolved/archive-driven-once.md), [Resolved #262](resolved/behind-asked-of-holders-first.md)), as are 272 and the first half of 264, whose second half is here ([Resolved #264, in part](resolved/archive-record-length-bound.md), [Resolved #272](resolved/archive-step-comments.md)), 265 and 266 ([Resolved #265](resolved/accept-counts-empty-chunks.md), [Resolved #266](resolved/narrowed-scrub-widens.md)), 267 and 268 together ([Resolved #267, 268](resolved/upgrade-keeps-the-file.md)), 269 ([Resolved #269](resolved/put-stale-on-every-exit.md)), 263 ([Resolved #263](resolved/archives-dealt-again.md)), 271 ([Resolved #271](resolved/lineage-judged-where-it-overlaps.md)), and 270 ([Resolved #270](resolved/no-committed-schema-refused.md)); 260 to 272 were filed by a code review of the branch from [F87](../features/tablet-walk.md) to [F93](../features/operations-and-the-real-cluster.md), each read again against the source by a second pass before it was filed, with [O117](optimizations.md#o117-an-archive-restore-reads-each-archive-two-or-three-times-and-copies-each-object-on-every-try) beside them, and 260 is a gap in 258's own fix that is older than it; 259 is on the resolved page with 258: [F93](../features/operations-and-the-real-cluster.md)'s acceptance rows found an idle table's checkpoint standing still while another table on its shard was written, so a backup of it never cut ([Resolved #258](resolved/idle-group-checkpoint.md)), and `force_recover` panicking on a control log applied to its end ([Resolved #259](resolved/force-recover-applied-log.md)), each filed and fixed in one change; 255, 256 and 257 are on the resolved page too, built by F93 as Q31 decided ([Resolved #255](resolved/restore-judged-by-table.md), [Resolved #256](resolved/stripe-ahead-of-its-row.md), [Resolved #257](resolved/restore-waits-for-stream-budget.md)); ~~item 257 is the newest number, filed with 255 and 256 by~~ 257 was filed with 255 and 256 by [Q31](../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)'s lab run, which reproduced the collision Q31 names before it was decided: a restore's install refused by its receiver's stream budget fails its group, which a retry by hand finishes; 256, a row older than its chunks answered as a retryable failure and never refused by name; and 255, a restore refusing a backup cut under another schema id, so that the path every page names for a schema change is refused; 254 was filed by [Resolved #253](resolved/put-left-on-moved-holder.md)'s lab work: a capture names the project's commit beside a node program the inventory names prebuilt; 253, on the resolved page: [F92](../features/reclamation-and-device-lifecycle.md)'s lab drain found a put through a stale placement leaving its chunk on a holder its group had moved off, so a drained device never emptied, filed and fixed in one change, and its late pieces, any late writer's and a refused put's in three more, one of them a sweep of every slice for stray groups ([Resolved #253](resolved/put-left-on-moved-holder.md)); ~~item 252 is the newest number, filed by~~ 252 was filed by [F92](../features/reclamation-and-device-lifecycle.md)'s rotational run: two of F91's refusal tests cannot pass under `SHOAL_TEST_ROTATIONAL=1`, whose harness gives every device a journal; ~~item 251 is the newest number, on the resolved page:~~ 251, on the resolved page: [F91](../features/rotational-devices.md) found a change of a slice's journal size zeroing its records at the next start, while it gave a journal a place of its own, and filed and fixed it in one change ([Resolved #251](resolved/journal-resized-over-records.md)); 250 was filed by [F90](../features/erasure-coded-pools.md#on-the-lab)'s lab: a placement group that never held a stripe keeps placing on a failed device, which at 2+1 refuses a put; 249 was filed by [F90](../features/erasure-coded-pools.md#tests)'s stress of its own test, eight standalone nodes at once on one host: an object executor's apply queue stopped being run, as it does at the commit before F90, since [resolved](resolved/an-executor-parked-with-work.md) in the glommio fork; 248 was filed by [F89](../features/scrub-and-repair.md#on-the-lab)'s lab figures: `shoaladm bench` left an empty device directory it had made on one host after a run; ~~item 247 is the newest number, filed by~~ 247 was filed by [F88](../features/recovery-and-moves.md#on-the-lab)'s lab figures: a failed device's pool gathered stale positions faster than it rebuilt them, until the one device left on that host filled, since nothing is reclaimed before M20; ~~item 246 is the newest number, on the resolved page:~~ 246, on the resolved page: the same run found a shard that never read a moved placement group placing by its pool's pin, so every shard's first put to each moved group left its arrival stale ([Resolved #246](resolved/moved-group-taken-at-base.md)); ~~item 245 is the newest number, on the resolved page:~~ 245, also on the resolved page: the same run found the control log's rewrite after a purge holding the log borrowed while a replication stream read it, which panicked the control thread of a node once moves took its log past five thousand entries ([Resolved #245](resolved/control-log-rewrite-borrow.md)); ~~item 244 is the newest number, filed by~~ 244 was filed by [F88](../features/recovery-and-moves.md)'s lab run: a load that saturates a 1 GbE link delays the control lane's reports, and the leader calls serving members silent; 243, on the resolved page, was found by the same run and filed and fixed in one change: a failed device's moves gave themselves up on a map that lagged their admission, and loaded the control group with slot commits and refusals ([Resolved #243](resolved/move-slots-on-the-map.md)); ~~item 242 is the newest number, on the resolved page:~~ 242, on the resolved page too: F88's lab run found a stream's task spawned with the open-stream map still borrowed, which glommio polls before `spawn_local` returns, so a first piece to a failed slice ended its node's object executor; filed and fixed in one change ([Resolved #242](resolved/stream-spawned-under-borrow.md)) (F88's acceptance tests filed none: the defects they found were in its own code, fixed before it was committed, and recorded on its page); ~~item 241 is the newest number, filed by~~ 241 was filed by [F87](../features/tablet-walk.md), whose walk merges what the scrub's cut and the digest do not: a sorted partition written before it was read is digested without its archived rows, reproduced by an ignored test; 240 is on the resolved page with 239: F86's patch leg found writes in place under contention stuck, because a record kept only the one label it was laid over and could not be applied once a fold moved its chunk ([Resolved #239](resolved/record-bases.md)), and a write whose row moved answered `NotReady` rather than trying again ([Resolved #240](resolved/moved-row-not-ready.md)), each filed and fixed in one change; 238, also on the resolved page, is that a put that outran 1 GbE was refused by remote holders that were keeping up, because the object lane refused a request at its bound at once, found by F86's three-node bench leg and filed and fixed in one change ([Resolved #238](resolved/object-lane-backpressure.md)); 237, on the resolved page too, is that a put past the threshold that reached a node before its object executors started was refused `TooLarge`, which a client never retries, found by [F86](../features/replicated-pools.md)'s one-node bench leg after F86 was committed and filed and fixed in one change ([Resolved #237](resolved/object-store-starts-after-the-shards.md)); 236, also on the resolved page ([F86](../features/replicated-pools.md)'s acceptance tests filed none: the defects they found were in its own code, fixed before it was committed, and recorded on its page), is that a replication link replaced after an address change could have its late answer complete the new link's RPC of the same id, found by [F84](../features/replication-lane-handoff.md) reading the map it rekeyed and filed and fixed in one change ([Resolved #236](resolved/replaced-link-answers.md)); 235 was filed by F84 too, by reading the acceptor it hands lanes from: the client acceptor drops the TLS session its comment says it keeps for the connection's life; ~~item 234 is the newest number, filed by~~ 234 was filed by [F83](../features/rack-and-zone-domains.md)'s workspace run: a plan's move can spend its stale-version retries under churn, and `automatic_removal_and_rejoin_preserve_fencing` counts the step that failed though the plan recovered; ~~item 233 is the newest number, filed by~~ 233 was filed by [F82](../features/root-capacity.md) while it made the reserve check judge every root: a snapshot install holds its stream twice, as its partial and in the archives, and the check counts it once; 232, on the resolved page, is a former control leader answering capacity it no longer heard, found by F82's roots section and filed and fixed in one change ([Resolved #232](resolved/former-leader-capacity.md)); 231, on the resolved page: the compactor's job loop had lost its `#[hotpath::skip]` in a rewrite, so a profile reported its lifetime as work; found by [Resolved #229](resolved/hotpath-build.md)'s test and filed and fixed in one change ([Resolved #231](resolved/compactor-loop-profiled.md)); 230, on the resolved page too, is that an object operation held its pooled connection until its answer came back, found by [F80](../features/object-wire.md)'s baseline and filed and fixed in one change ([Resolved #230](resolved/object-connection-held.md)); 229, ~~open~~ on the resolved page too, is that the `hotpath` build of the engine did not compile, found by [F80](../features/object-wire.md) running the runbook's feature lines and older than buckets: F76's paged index put a run's write under the archive map's commit, and the compactor's futures went past the compiler's query depth once `hotpath` wrapped each method in one more ([Resolved #229](resolved/hotpath-build.md)); 228, on the resolved page, is that a row holding a `u128` behind a pointer, as an object's entries hold its id, could end its shard at a restart, because the intent log and the archives laid records back to back and rkyv validates a `u128` at a sixteen byte boundary; found by [F80](../features/object-wire.md)'s acceptance test and filed and fixed in one change ([Resolved #228](resolved/aligned-records.md)); 227, also on the resolved page, is that the `stage-profile` build of the engine had not compiled since F75, found by [F79](../features/bucket-schema.md) running the runbook's feature-gated lines and fixed with a test that holds the durability phases it fills ([Resolved #227](resolved/stage-profile-build.md)); 226 is `compare` refusing two captures of one benchmark because each node's memory moved by kilobytes with a kernel update, found by [F78](../features/object-harness.md)'s lab check; 225 is a full load stalled after every row was acknowledged, met once by [F77](../features/authorization.md)'s lab work and not reproduced; 224 is a strong read under load refused because its leader's heartbeat round found no quorum, measured by F74's lab A/B on both of its sides; 223, on the resolved page: F74's lab A/B found a routed client's strong reads following a leader hint after its lead had moved back, filed and fixed in one change ([Resolved #223](resolved/leader-hints-lapse.md)); 222 was filed with 221 by [F74](../features/client-routing.md), which found both by reading the server for what client routing relies on: a strong exists takes no barrier, and a second forward of a bundle on one peer connection is counted without its bytes; 220, on the resolved page, was found and fixed by F74 too: a client never retried a query refused as routed by a stale map ([Resolved #220](resolved/stale-topology-retried.md)); 219 was filed when the contract was agreed: the tablet model's saved schedules drifted from what their builder writes; 218, on the resolved page, is a fault in the lab's before-and-after procedure, which ran a node with its client on a shard's other thread and no buffers registered, found by [X9](../object-storage/table-latency.md) and filed and fixed in one change ([Resolved #218](resolved/lab-core-layout.md)); 214, 215 and 216 were filed by [X3](../object-storage/bytes-through-groups.md) and 217 by [X8](../object-storage/small-writes.md), ~~all four open~~ the first three open, and 217 [resolved](resolved/owed-connection-kept.md) with 317, one defect filed twice; 213 is a defect in X11's spike harness, found while planning X13 and filed and fixed in one change ([Resolved #213](resolved/x11-setup-fifo.md)); 212, ~~open~~ on the resolved page, was filed by [X2](../object-storage/placement-simulation.md#the-logarithm), which found it choosing placement's logarithm, and resolved by [F85](../features/devices-and-pools.md), which built that logarithm ([Resolved #212](resolved/lead-score-logarithm.md)); 211 was filed by [Resolved #210](resolved/bench-preload-frame.md), whose reproduction found it, with 210 on the resolved page (209 was filed with 208 by [Resolved #202](resolved/append-batch-bytes.md), 208 in the place 202 left and 209 from its lab check; 207 was filed by [Resolved #92, #198](resolved/composite-partition-key.md) beside item 42 that it belongs with) (202 to 206 were filed by the [object storage plan](../object-storage/prerequisites.md): four found by reading the code it would be built on, and the last by checking its own links; 199, 200 and 201 stay on the resolved page, and 92 and 198, one defect filed twice, joined them in one change as [Resolved #92, #198](resolved/composite-partition-key.md), 46 and 202 joined them as [Resolved #46](resolved/unmarked-directory-refused.md) and [Resolved #202](resolved/append-batch-bytes.md), and 93, which the plan listed beside them as optional, joined them as [Resolved #93](resolved/archived-partition-hash.md)), and why 16 and 112 are on the resolved page beside them, and why 17, 30, 33, 43, 78, 79, 80, 82,
83, 84, 85, 86, 88, 89, 90, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 115, 116, 120, 121, 122, 123, 124, 125, 126, 127 and 128 are on the resolved page, with 27, 32 and 36 beside them. **126 never appeared here**:
it was filed and fixed in one change, from a user's run of `tmdb_dataset` on a host without
`/opt/shoal` ([Resolved #126](resolved/storage-directory-unusable.md)). **127 never appeared here**
either: it was filed and fixed in one change, from a user's bootstrap of a wizard-built inventory
([Resolved #127](resolved/wizard-loopback-address.md)). **128 never appeared here** either:
it was found by the same user's TMDB load against the lab, reproduced by a fixture test, and
fixed in the change that filed [item 129](resolved/overload-sheds.md)
beside it ([Resolved #128](resolved/hop-deadline-margin.md)). **120 and 121 never appeared here**:
they were found by reading the code around item 30 - the unsorted table's version of its arm
charging the shard twice, and the duplicate read that let either arm be reached - reproduced by
a harness that builds a shard's tables without a shard, and fixed with it
([Resolved #30, 120, 121](resolved/resident-copy-collision.md)). **115 and 116 never appeared here**:
they were the two fixture tests F52's whole-workspace run failed and each passed alone -
a crash between the retry sidecar and the checkpoint file forgetting every identity below
the checkpoint, reproduced by a crash point between the two
([Resolved #115](resolved/retry-sidecar-crash-window.md)), and a test reading the newest map
version once while the detector committed another
([Resolved #116](resolved/map-version-test-snapshot.md)) - and were fixed in the change that
filed them. **114 never appeared here**: it was found by
[F51](../features/cluster-deployment.md)'s deployment test - the cluster tab's model counted
no voters in any cluster - reproduced against a real node and by a unit test, and fixed in the
same change ([Resolved #114](resolved/cluster-tab-voter-count.md)). **111 never
appeared here**: it was found by [F47](../features/local-rehome.md)'s crash matrix - a read
landing while an archive closed panicked the executor - reproduced against the map alone and
fixed in the same change ([Resolved #111](resolved/archive-removal-borrow.md)). **108 never
appeared here**: it was found by [F45](../features/replica-migration.md)'s first smoke run and
fixed in the same change ([Resolved #108](resolved/cluster-arm-overrides-dropped.md)). **99 moved at M8**
([Resolved #99](resolved/durable-log-reversion.md)): it was filed at M6, left for M8 by M7 on
purpose, and reproduced before it was fixed. **101 never appeared here
either**: it was found by an M6 test and fixed in the same change
([Resolved #101](resolved/short-lived-member-detection.md)), reproduced first. **104 and 105
never appeared here either**: both were found by reading the code for M7, reproduced by a
fixture test each, and fixed before the milestone's own work began
([Resolved #104](resolved/segments-recompacted-after-restart.md),
[Resolved #105](resolved/volatile-groups-never-purged.md)). **33 moved at M5**
([Resolved #33](resolved/gather-expiry.md)): it was the oldest open hang, and it was reproduced
before it was fixed. **94 never appeared here either**: it
was found and fixed while [F38](../features/inter-node-transport.md) made a peer link a client,
and was the first half of item 32 ([Resolved #94](resolved/disconnected-client-cleanup.md)), whose
second half is now [resolved](resolved/client-gone-broadcast.md) too. **38, 58 and 88 moved together**
([Resolved #38, 58, 88](resolved/pool-readiness.md)): three symptoms of one cause, the pool
returning before its shards had bound. **91 was found by fixing them** — the honest `exit` that
closed 58 surfaced a compactor that had been dying silently for as long as one test had existed. **79, 82, 83, 84, 85 and 86 never appeared here at all**: each was
found and fixed in the same change ([Resolved #79](resolved/micro-only-capture-current.md),
[Resolved #82](resolved/one-line-per-capture.md),
[Resolved #83](resolved/default-metric-half-the-corpus-cannot-answer.md),
[Resolved #84](resolved/identical-facts-one-line.md),
[Resolved #85](resolved/chart-framed-for-the-last-selection.md),
[Resolved #86](resolved/hover-label-reads-in-decades.md)), which is allowed and is
worth noting because it makes the numbering look like six entries went missing. Item 80 is the other way round —
it was filed here rather than fixed, because the fix turned on a question about the storage layer
that reading `block_on_load` alone could not answer, and stayed here until somebody answered it
([Resolved #80](resolved/never-flushed-partitions.md)). The exceptions are items 20, 24, ~~32,~~ 54, 73, ~~250~~, 264 and 286, which were only
partly fixed: the open remainder is here and the rest is there. Items 9, 15, 16, 32, 51 and 250 were each one such
exception until their last part was fixed, and are now on the resolved page alone - 16 in four
parts, the last of which ([Resolved #16](resolved/hot-path-panics.md)) also found that the item's
write path claim was wrong and filed what it should have said as item 122, since
[resolved](resolved/intent-log-failure.md) with 123 and 124 beside it; item 25 was one
in the other direction — it had one row left open, that row was fixed, and the whole item
[moved](resolved/claude-md-drift.md).

**Baseline as of writing:** `cargo check --workspace --all-targets` passes with warnings;
`cargo test --workspace` passes — ~~**1,238 tests**~~ ~~**1,289 tests**~~ ~~**1,320 tests**~~ ~~**1,342 tests**~~ ~~**1,361 tests**~~ ~~**1,382 tests**~~ ~~**1,398 tests**~~ ~~**1,414 tests**~~ ~~**1,432 tests**~~ ~~**1,449 tests**~~ ~~**1,467 tests**~~ ~~**1,475 tests**~~ ~~**1,484 tests**~~ ~~**1,492 tests**~~ ~~**1,529 tests**~~ ~~**1,541 tests**~~ ~~**1,543 tests**~~ ~~**1,549 tests**~~ ~~**1,555 tests**~~ ~~**1,564 tests**~~ ~~**1,576 tests**~~ ~~**1,587 tests**~~ ~~**1,589 tests**~~ ~~**1,605 tests**~~ ~~**1,608 tests**~~ ~~**1,611 tests**~~ ~~**1,614 tests**~~ ~~**1,619 tests**~~ ~~**1,629 tests**~~ ~~**1,633 tests**~~ ~~**1,634 tests**~~ ~~**1,635 tests**~~ ~~**1,636 tests**~~ ~~**1,637 tests**~~ ~~**1,639 tests**~~ ~~**1,640 tests**~~ ~~**1,641 tests**~~ ~~**1,642 tests**~~ ~~**1,644 tests**~~ ~~**1,647 tests**~~ ~~**1,651 tests**~~ ~~**1,653 tests**~~ ~~**1,654 tests**~~ ~~**1,655 tests**~~ ~~**1,656 tests**~~ ~~**1,661 tests**~~ ~~**1,663 tests**~~ ~~**1,674 tests**~~ ~~**1,685 tests**~~ ~~**1,691 tests**~~ ~~**1,715 tests**~~ ~~**1,725 tests**~~ ~~**1,726 tests**~~ ~~**1,728 tests**~~ ~~**1,782 tests**~~ ~~**1,785 tests**~~ ~~**1,790 tests** since [F65](../features/query-figures-home-tab.md), eleven ignored~~ ~~**1,892 tests** since [F67](../features/bench-run-wizard.md)~~ ~~**1,894 tests** since [Resolved #201](resolved/attached-read-order.md)~~ ~~**1,911 tests** since [F68](../features/conditional-writes.md)~~ ~~**1,921 tests** since [Resolved #92, #198](resolved/composite-partition-key.md)~~ ~~**1,930 tests** since [Resolved #46](resolved/unmarked-directory-refused.md)~~ ~~**1,934 tests** since [Resolved #202](resolved/append-batch-bytes.md)~~ ~~**1,942 tests** since [F69](../features/driver-operation-kinds.md)~~ ~~**1,947 tests** since [F70](../features/storage-faults.md)~~ ~~**1,949 tests** since [Resolved #210](resolved/bench-preload-frame.md)~~ ~~**1,962 tests** since [F71](../features/bench-device-memory.md) and [F72](../features/bench-paced-stream.md)~~ ~~**2,021 tests** since [F73](../features/bodies-across-frames.md), with twenty that the X10 and X11 spikes had added uncounted,~~ ~~**2,022 tests** since [Resolved #213](resolved/x11-setup-fifo.md),~~ ~~**2,029 tests** since [X13](../object-storage/benchmark-shape.md),~~ ~~**2,062 tests** since [X1](../object-storage/stripe-model.md),~~ ~~**2,081 tests** since [X3](../object-storage/bytes-through-groups.md),~~ ~~**2,114 tests** since [X8](../object-storage/small-writes.md), unchanged by [X9](../object-storage/table-latency.md),~~ ~~**2,126 tests** since [X12](../object-storage/recovery-scrub-rates.md),~~ ~~**2,138 tests** since [the contract was agreed](../object-storage/contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09),~~ ~~**2,161 tests** since [F74](../features/client-routing.md)~~ ~~**2,163 tests** since [Resolved #223](resolved/leader-hints-lapse.md)~~ ~~**2,200 tests** since [F75](../features/client-cancel.md)~~ ~~**2,213 tests** since [F76](../features/paged-archive-map.md)~~ ~~**2,232 tests** since [F77](../features/authorization.md)~~ ~~**2,245 tests** since [F78](../features/object-harness.md), with [Resolved #93](resolved/archived-partition-hash.md)'s two doctests~~ ~~**2,265 tests** since [F79](../features/bucket-schema.md)~~ ~~**2,266 tests** since [F79's lab figures](../features/bucket-schema.md#performance)~~ ~~**2,308 tests** since [F80](../features/object-wire.md)~~ ~~**2,309 tests** since [Resolved #230](resolved/object-connection-held.md)~~ ~~**2,318 tests** since [F81](../features/failure-domains.md)~~ ~~**2,326 tests** since [F82](../features/root-capacity.md)~~ ~~**2,331 tests** since [F83](../features/rack-and-zone-domains.md)~~ ~~**2,341 tests** since [F84](../features/replication-lane-handoff.md)~~ ~~**2,421 tests** since [F85](../features/devices-and-pools.md)~~ ~~**2,465 tests** since [F86](../features/replicated-pools.md)~~ ~~**2,466 tests** since [Resolved #237](resolved/object-store-starts-after-the-shards.md)~~ ~~**2,467 tests** since F86's bench read its pool devices~~ ~~**2,468 tests** since [Resolved #238](resolved/object-lane-backpressure.md)~~ ~~**2,471 tests** since [Resolved #239](resolved/record-bases.md)~~ ~~**2,495 tests** since [F87](../features/tablet-walk.md)~~ ~~**2,529 tests** since [F88](../features/recovery-and-moves.md)~~ ~~**2,530 tests** since [Resolved #242](resolved/stream-spawned-under-borrow.md)~~ ~~**2,531 tests** since [Resolved #245](resolved/control-log-rewrite-borrow.md)~~ ~~**2,549 tests** since [F89](../features/scrub-and-repair.md)~~ ~~**2,577 tests** since [F90](../features/erasure-coded-pools.md)~~ ~~**2,594 tests** since [F91](../features/rotational-devices.md)~~ ~~**2,615 tests** since [F92](../features/reclamation-and-device-lifecycle.md)~~ ~~**2,617 tests** since [Resolved #253](resolved/put-left-on-moved-holder.md)~~ ~~**2,619 tests** since [Resolved #253](resolved/put-left-on-moved-holder.md)'s sweep~~ ~~**2,620 tests** since [Resolved #253](resolved/put-left-on-moved-holder.md)'s refused put~~ ~~**2,651 tests** since [F93](../features/operations-and-the-real-cluster.md)~~ ~~**2,669 tests** since [Resolved #260 to #273](resolved-issues.md)~~ ~~**2,699 tests** since [F94](../features/bench-object-endpoints.md)~~ ~~**2,701 tests** since [F95](../features/bench-object-read-levels.md)~~ ~~**2,713 tests** since [F96](../features/chunk-file-classes.md), 2,712 counted by a workspace run and one added after it~~ ~~**2,714 tests** since [F97](../features/disk-reads-by-queue.md), counted by a workspace run~~ ~~**2,718 tests** since [F99](../features/strong-read-rounds.md)~~ ~~**2,720 tests** since [Resolved #283](resolved/chained-records-decided-together.md)~~ ~~**2,722 tests** since [F100](../features/lease-reads.md)~~ ~~**2,725 tests** since [F101](../features/disk-read-margin.md), counted from its diff and each binary's `--list`,~~ ~~**2,726 tests** since [F98](../features/commit-delay-bounded-by-the-sync.md), counted by its workspace run,~~ ~~**2,733 tests** since [F102](../features/read-unit-cache.md), counted by its workspace run,~~ ~~**2,744 tests** since [F104](../features/memory-in-a-capture.md), counted by its workspace run, which counted Resolved #299's and F103's too,~~ ~~**2,759 tests** since [F107](../features/cache-default-share.md), counted by its workspace run, which counted F105's, F106's and Resolved #301's too,~~ ~~**2,762 tests** since [Resolved #304](resolved/lone-frame-loss-probe.md), counted by its workspace run,~~ ~~**2,763 tests** since Resolved #304 was scoped to the lanes of rounds, counted from its diff after its workspace run,~~ ~~**2,764 tests** since Resolved #304's client connections hold its floor until they carry an object, counted from its diff after its workspace run,~~ ~~**2,798 tests** since wave three's window of the store comparison's optimizations (Resolved #303, F108, Resolved #307 to #310, F109, F110, Resolved #313, F111 and F112), counted by its workspace run and matching their diffs,~~ ~~**2,802 tests** since F113 and the store comparison's renderer, counted by their workspace run and matching their diffs,~~ ~~**2,803 tests** since Resolved #318, counted from its diff after F113's workspace run,~~ ~~**2,804 tests** since Resolved #319, counted from its diff after F113's workspace run,~~ ~~**2,805 tests** since Resolved #317, counted from its diff after F113's workspace run,~~ ~~**2,806 tests** since Resolved #316's bound, counted from its diff after F113's workspace run,~~ ~~**2,808 tests** since Resolved #315, counted from its diff after F113's workspace run,~~ ~~**2,809 tests** since Resolved #314, counted from its diff after item 315's workspace run, which counted 2,808,~~ ~~**2,811 tests** since Resolved #322 in part, counted from its diff after item 315's workspace run, which counted 2,808,~~ ~~**2,825 tests** since Resolved #324, counted by the found items' workspace run with `shoal-core`'s lib suite run again whole,~~ ~~**2,828 tests** since Resolved #297 and #327, counted from their diff after the found items' workspace run,~~ ~~**2,829 tests** since Resolved #281, counted from its diff after the found items' workspace run,~~ ~~**2,830 tests** since Resolved #325, counted from its diff after the found items' workspace run,~~ ~~**2,831 tests** since Resolved #291, counted from its diff after the found items' workspace run,~~ ~~**2,833 tests** since Resolved #312, counted from its diff after the found items' workspace run,~~ ~~**2,834 tests** since Resolved #284, counted from its diff after the found items' workspace run,~~ ~~**2,835 tests** since Resolved #289, counted from its diff after the found items' workspace run,~~ ~~**2,838 tests** since Resolved #328, counted from its diff after the found items' workspace run,~~ ~~**2,840 tests** since item 142's hold, counted from its diff after the found items' workspace run,~~ ~~**2,841 tests** since Resolved #330, counted from its diff after the found items' workspace run,~~ ~~**2,842 tests** since Resolved #331, counted from its diff after the found items' workspace run,~~ ~~**2,847 tests** since Resolved #250's rest and #333, counted from their diff after the found items' workspace run,~~ ~~**2,850 tests** since Resolved #336, counted from its diff after workspace run two, which counted 2,847,~~ ~~**2,852 tests** since Resolved #337, counted from its diff after workspace run three, which counted 2,850,~~ **2,885 tests** since F115 to F119 and X16's crate, counted by the workspace run at `1aded160`, 2,885 passed and none failed, ~~twelve~~ ~~thirteen~~ ~~fourteen~~ ~~fifteen~~ ~~sixteen~~ ~~seventeen~~ ~~eighteen~~ ~~nineteen~~ twenty ignored (F66 took it to 1,873 without moving this line), plus ~~13~~ ~~14~~ 15
more behind `--features stage-profile`, ~~7 behind `--features x9`~~ 3 behind `--features object-load` (X9's seven went with its code in F85) 1 behind `--features hotpath` ([Resolved #229](resolved/hotpath-build.md)) and ~~11~~ 12 behind `--features s3` ([F94](../features/bench-object-endpoints.md), [F95](../features/bench-object-read-levels.md)) that a default run does not reach ([Test Coverage](test-coverage.md)) -
with the fixture binary run at `--test-threads 6`, since at the default thirty-two nineteen of
its ~~fifty-four~~ ~~sixty-four~~ ~~seventy-one~~ ~~eighty~~ ~~eighty-nine~~ ~~ninety-two~~ ~~ninety-five~~ ninety-seven fail under the load (item 100) and every one of them passes at six;
two of `persistent_unsorted_table.rs` fail about one run in five of that binary (item 107).
[F53](../features/inventory-wizard.md) added 16 and took it to 1,605: thirteen `shoalctl` unit
tests, a new binary `shoaladm/tests/wizard.rs` (2), and one in `deploy_render.rs`. The workspace
run at six threads with `--no-fail-fast` passed every test.
[Resolved #127](resolved/wizard-loopback-address.md) added 3 `shoalctl` unit tests and took it to
1,608, and [F54](../features/tmdb-dataset-deployment.md) 2 in the new `tmdb-dataset` crate and an ignored doctest,
to 1,611. [Resolved #128](resolved/hop-deadline-margin.md) added 3 (a fixture test, a `shoal-core`
unit test, a `tmdb-dataset` unit test) and took it to 1,614. Its workspace run failed eight
fixture tests: two passed alone, and six could not bind ~~port 12000~~ ports 12001 and 12002
because a deployed lab node held them on the same host ([Test Coverage](test-coverage.md)), since
[resolved](resolved/fixture-default-peer-ports.md) as item 134. The
[distributed cluster testing](../cluster-testing/overview.md) chapter's fixes (items 60, 130, 131,
133 to 157, O61, O62, O63, O65 to O68) and its driver added 37 and took it to 1,656 ([Test Coverage](test-coverage.md)). [Resolved #158](resolved/runtime-waker-lists.md) added 5 `shoal-core` unit tests and took it to 1,661, every one passing at six threads. [Resolved #159](resolved/map-ahead-of-archive.md) added 2 and took it to 1,663. Its run at six threads failed two fixture tests from item 142's list, each of which passed alone three times. Items 160 to 168 and [F56](../features/cluster-rebuild.md) added 11 and took it to 1,674 ([Test Coverage](test-coverage.md)). Items 169 to 178 added 10 and took it to 1,685. #176, #179 and O74 added 6 and took it to 1,691. The distributed cluster testing chapter's round 11 (#129, #142's assertion, #143, #156, #181, #182, O57, O74, O76 and [F57](../features/cluster-reconfigure.md)) added 10 and took it to 1,701. Its workspace run at six threads passed every one. Rounds 12 to 14 took it to 1,728 ([Test Coverage](test-coverage.md)). Round 15 (F61, #191, #192, #194, #195, O81 to O83) added 14 and took it to 1,742; its workspace run at six threads on the final code passed every one. [Resolved #92, #198](resolved/composite-partition-key.md) added 10 and took it to 1,921. Its workspace run at six threads, with the build held to four jobs because europa's btrfs root stalled its linkers at full parallelism, passed 1,920: `single_node_data_has_a_verified_cluster_migration_path` failed with *267 against 400*, the #142 deadline, and passed alone three times. [Resolved #46](resolved/unmarked-directory-refused.md) added 9 and took it to 1,930. [Resolved #202](resolved/append-batch-bytes.md) added 4 and took it to 1,934. [F69](../features/driver-operation-kinds.md) added 8 and took it to 1,942. [F70](../features/storage-faults.md) added 5 and one ignored and took it to 1,947. Its workspace run at six threads passed 1,945; the two fixture failures, `single_node_data_has_a_verified_cluster_migration_path` at the #142 deadline and `node_transfer_budgets_bound_concurrent_sources`, each passed alone, and another run swapped the second for `leads_follow_the_members_lead_weights`, which passed alone too.
[Resolved #27](resolved/shql-quote-escape.md), [#32](resolved/client-gone-broadcast.md),
[#36](resolved/staged-tail-deadline.md) and [#125](resolved/retry-unknown-outcome.md) added 11
and took it to 1,587. There are two new binaries, `retry_outcome.rs` (3) and `staged_flush.rs`
(1), plus one test in each of `shql.rs` and `client_disconnect.rs`, four in `shoal-proto` and
one in `shoal-client`. The workspace run at six threads with `--no-fail-fast` failed two fixture
tests, and each passed when run alone: `a_dead_primary_fails_writes_only_until_its_election`
(item 100's shape) and `local_rehome_recovers_after_each_crash_point`, which no earlier run had
recorded failing under load.
[Resolved #122](resolved/intent-log-failure.md), [#123](resolved/parked-get-key.md) and
[#124](resolved/unsorted-update-generation.md) added 12 and took it to 1,576: one new binary,
`intent_log_failure.rs` (4), five in `resident_reads.rs`, and three `shoal-core` unit tests. They
filed item 125. The workspace run at six threads with
`--no-fail-fast` passed every test. An earlier fail-fast run failed
`a_dead_primary_fails_writes_only_until_its_election` (item 100's shape).
[Resolved #16](resolved/hot-path-panics.md) added 9 and took it to 1,564: one new binary,
`hot_path_failures.rs` (5, on the `table_backlog.rs` harness), and four unit tests in
`shoal-core`; one `shoal-proto` test moved its probe of an unclaimed code from 13, which
`StorageWrite` now claims, to 14. It filed items 122, 123 and 124. Under the workspace run at six
threads three fixture tests failed and each passed alone:
`a_dead_primary_fails_writes_only_until_its_election`,
`lost_response_retry_returns_original_result` and
`scheduled_scrub_quarantines_without_an_operator` (item 100's shape).
[The remainder of item 15](resolved/backlog-bounds.md) added 6 and took it to 1,555, one new
binary, `table_backlog.rs`, and one test in `backpressure.rs`, and filed nothing. Under the
workspace run at six threads three fixture tests failed across two runs and each passed alone:
`backup_restore_verifies_history_in_new_cluster` (item 117), `down_retains_placement_during_grace`
(item 119) and `migration_resumes_after_each_phase_failure` (item 100).
[Resolved #30, 120, 121](resolved/resident-copy-collision.md) added 6 and took it to 1,549,
one new binary, `resident_reads.rs`, and filed nothing: the two defects it found beside item 30
were fixed with it. Under the workspace run at six threads two fixture tests failed and passed
alone (item 100).
[Resolved #115](resolved/retry-sidecar-crash-window.md) and [Resolved #116](resolved/map-version-test-snapshot.md)
added 2 and took it to 1,543, filing items 117, 118 and 119: one fixture test each that
failed under load in their runs and passed alone.
[F50](../features/cluster-operations.md) added 8 and took it to 1,492, filing nothing and
resolving nothing here: the two defects it met - the control leader dialling a moved member at
the address it was admitted with rather than the one it committed, and a winning clone's
shorter log stopping the leader's control thread on a debug assertion once the leader did
reach it - were found by the M3 fencing row and its own address row and designed out before
the page was written, and are recorded on the F page. Its gate table names items 100, 102,
106, 107, 109 and 110 as open and not gates. Under the workspace run at six threads three
fixture tests failed, two of them item 100's, and under the fixture suite on its own two did
(item 100).
[F49](../features/backup-and-recovery.md) added 9 and took it to 1,484, filing nothing and
resolving nothing: the three defects it met - a recovered survivor's state diverging from a
joiner's replay of the same log, two plans planned in one pass picking the same destination
for one set, and a move re-adding a member a recovery had rewritten out of a group - were
found by its own fixture test and designed out before the page was written, and are recorded
on the F page. Under the workspace run at six threads one of F46's fixture tests failed and
passed alone, and under the fixture suite on its own four did (item 100).
[F48](../features/rolling-compatibility.md) added 8 and took it to 1,475, filing nothing and
resolving nothing: the one defect it met - a control state persisted by an older build
dropping a field the newer one judged by - was found by the run against a real previous
binary and designed out before the page was written, and is recorded on the F page rather
than here. Under the workspace run at six threads one of F46's fixture tests failed and
passed alone, and under the fixture suite on its own one of F47's did the same (item 100).
[F47](../features/local-rehome.md) added 18 and took it to 1,467, resolving item 111 on the
way - found by its crash matrix, reproduced first - and filing nothing new: what it left undone
is on the todos page, and item 43 now says what the rehome does not cover.
[F46](../features/capacity-rebalancing.md) added 17 and took it to 1,449, filing item 110 on the
way; this line did not say so until F47, which is the staleness the paragraph below warns of.
[F45](../features/replica-migration.md) added 18 and took it to 1,432, resolving item 108 on
the way and filing 109.
[F44](../features/repair.md) added 16 and took it to 1,414, resolving item 99 before its own
work began and filing nothing new: what it left undone is on the todos page.
[F43](../features/node-recovery.md) added 16 and took it to 1,398, resolving items 104 and 105
before its own work began and filing 106 and 107 on the way.
[F42](../features/primary-failover.md) added 21 and took it to 1,382, resolving item 101 and
filing 102 and 103 on the way.
[F37](../features/node-identity-control-plane.md) took the total to 1,265 and did not update this
line; [F38](../features/inter-node-transport.md) added 24 more and did;
[F39](../features/membership.md) added 31 and took it to 1,320;
[F40](../features/replication.md) added 22 and took it to 1,342, filing items 99 and 100 on
the way - the second of which is why the fixture suite is run at `--test-threads 6`.
[F36](../features/cluster-harness.md) added 40 — 28 of them in the new `shoal-model` crate, 4 in
the new `cluster_fixture.rs` binary whose two ignored functions are the children it re-executes
the binary as, and the rest over the pool's readiness handle, the frozen ports, the cluster record
and the distributed chapter's acceptance tables — and closed items 17, 38, 58 and 88 while
filing 91.
~~1,045~~ ~~1,084~~ ~~1,164~~ ~~1,187~~ ~~1,198~~ — this figure had gone stale by two features while the sentences below
it kept naming what each added, which is what a running total is supposed to prevent, and it then
went stale by four more in exactly the same way. It is re-derived from a run
rather than incremented, and [Test Coverage](test-coverage.md) is the page that carries the
per-binary breakdown. It had gone stale by a **whole feature** again this time:
[F34](../features/benchmark-tracing.md) added 20 and moved the figure on the coverage page and not
on this one, so 1,164 was two changes behind rather than one.
[Resolved #89](resolved/fragmented-query-traces.md) and
[Resolved #90](resolved/divergent-layer-filters.md) added the other 3 — a new
`tracing_topology.rs` integration binary carrying 1, which fails against the tree before the fix
and reports the three separate traces one query produced, and 2 `shoal-core` unit tests.
[F35](../features/wire-trace-context.md) added 11, and one of them is the first test here whose
existence depends on a feature **another crate** turns on: `trace_propagation.rs` is gated on
`otel`, which `cargo test -p shoal` does not enable and a `--workspace` run does, through
`shoal-bench`. So this figure counts it and a per-package run would not
([Test Coverage](test-coverage.md) says which is which).
[Resolved #76](resolved/stage-join.md) added 6, and moved the count in both directions at once: 2
in a default run and 4 behind the feature, including the first test anywhere that starts a server
under `stage-profile` and reads what it wrote. That is the test the stage layer never had, and its
absence is why three of four reports in `f22-row-size` were empty.
[F22](../features/row-size-benchmarks.md) added 21, two of which reproduce items 73 and 74 and fails against
the tree before its fix. [F23](../features/self-sizing-staging-buffer.md) added 7, one of which
reproduces [O34](optimizations.md) and fails against the tree before its fix — the first entry from
the optimizations page ever reproduced by a test rather than argued from source and sized by a
capture. [F25](../features/read-buffers-are-filled-not-zeroed.md) and
[Resolved #80](resolved/never-flushed-partitions.md) added 1, which fails against the tree before
the fix and is the first test anywhere that counts what the engine asked storage rather than what
it answered a client.
[Resolved #79](resolved/micro-only-capture-current.md) added 8, two of which fail against the tree
before the fix. [F26](../features/archive-routed-requests.md) added 5, four of which were confirmed
by breaking the code under them rather than by being written after it.
**Unchanged by the `f22-row-size` capture**, which added no tests and moved no count, and which is
where item [75](#75-a-control-that-was-measured-at-every-width-is-not-drawn-and-the-caption-says-it-is)
and [item 76](resolved/stage-join.md) came from — both read out of the committed artifact rather
than out of the source, which is the reverse of how everything above them was found and the reason
neither was caught earlier.
**Unchanged by `f23-staging-buffer` too**, which is where
[item 77](#77-a-macro-only-capture-can-never-reach-the-pages-it-was-taken-for) came from — found by
rendering the pages with that capture committed and noticing that none of them drew it. A third way
of finding a defect, after reading the source and reading an artifact: running the tool that
consumes one.
**Unchanged by the `F20-conf` capture** before it, which added no tests and moved no count: a capture is
evidence rather than a test, and what it produced was a reproduction for
[item 71](#71-throughput_sensitive-is-configured-documented-and-mostly-unused), a second and worse
reproduction for [item 58](resolved/pool-readiness.md), since resolved, and
[item 72](#72-an-arm-with-no-writes-is-reported-as-an-arm-that-ran-once) filed from reading the
renderer against the page it had just produced.
That is up from 986 with [F20](../features/configuration-sweeps.md) and
[F21](../features/benchmark-groups.md), which added 29 between them, all in `shoal-bench`. Before
that it was up from 967 with [F19](../features/chart-legends.md), which added 16 `shoal-bench` unit
tests over the shared chart legend, the data-derived axis ticks and the encryption charts in
nanoseconds, 2 integration tests over the legend's geometry, and 1 doctest. That is up from 896
with [F17](../features/workload-grid.md) and [F18](../features/results-pages.md), which added 71.
That is up from 876 with [F16](../features/client-builder.md), which added 12 `shoal-client` unit
tests over the builder and the endpoint order, a new `pool.rs` integration binary carrying 6, and 2
doctests. That is up from 868 with [F15](../features/client-server-split.md), which also
re-attributed 177
of them: `shoal-core` went from 323 unit tests to 146 as the protocol and the client became crates
of their own, and none were lost. That is up from 490, 301 and 25 with
[F14](../features/encryption-in-transit.md) — one new integration binary (`tls.rs`) carrying 8
tests, 17 unit tests over the TLS configuration and the kernel key material, 5 over the config
section, and 4 new doctests. The 8 in `tls.rs` and one of the 17 need the `tls` kernel module and
skip loudly without it, the same way the `stage-profile` tests sit outside a default run. No new
defect was filed while building it; item 64 was filed just before it, while correcting
[D4](../direction/encryption.md). Before that it was 482, 301 and 24 with
[F13](../features/transport-workloads.md) — 8 unit tests in `shoal-bench` over the eight transport
workloads and one doctest over `seed_batch`, with the `shoal-core` count unmoved because no engine
code changed. No new defect was filed while writing it. Item 64 was filed later, while correcting
[D4](../direction/encryption.md), and changes no count — it is a documentation defect and no test
was added or could be. Before that it was 473, 272 and 21 with
[F12](../features/authentication.md) — one new integration binary (`auth.rs`) carrying 9 tests, 17
unit tests over the mechanism and its credential store, 8 over the auth frame codec and the
handshake's new fields, 4 over the config section, and 3 new doctests. Item 63 was filed while
writing it, from reading the source. Before that it was 470, 258 and 21 with
[F11](../features/error-channel.md) — one new integration binary (`errors.rs`) carrying 3 tests,
7 unit tests over the error codec, 3 over the response payload, and 4 over the client's read path
and its dead connection sweep. Four existing integration tests changed what they assert rather than
being added to, which is the signal that the behaviour they pinned is the behaviour that moved.
Item 62 was filed while writing it, from reproducing it, and its reproduction is not committed —
the test that found it asserted behaviour F11 does not provide.
Before that it was 456, 231 and 21 with
[F10](../features/framing-and-protocol-evolution.md) — three new integration binaries
(`framing.rs`, `handshake.rs`, `fingerprint.rs`) carrying 14 tests between them, 23 unit tests over
the frame codec and the fingerprint, 2 over the client's read path, and 2 over the frame bound's
config default.
Before that it was re-run and re-counted binary by binary in August 2026, unchanged — items 59 and
60 are filed from reading and neither added a test.
That is up from 454, 229 and 21 with
[Resolved #57](resolved/missing-archive.md) — one integration test per persistent table over a
read whose archive is not on disk, and two unit tests over `get_archive` and how the failure it
now reports is classified. Before that it was up from 452, 225 and 21 with
[Resolved #16, 51](resolved/partition-load-failure.md) — two integration tests over a partition
read that fails and four unit tests over how a read failure is classified. Before that it was up
from 410, 219 and 21 with
[F9](../features/ephemeral-tables.md), and before that from 359, 219 and 16 with
[F8](../features/purpose-built-workloads.md), whose count moved in **both** directions — it
deleted a comparison engine along with its tests and moved others between crates, which that page
accounts for line by line. Before that it was up from 172, 215 and 11 with the stamp and
offset tests added by [F6](../features/stage-breakdown.md). Before that it was up from 168, 194 and 11 with the config and cpu selection tests
added by [items 18 and 50](resolved/excluded-cores-typo.md) and the baseline versioning and
throughput tests added by [F3](../features/performance-harness.md). Before those it was up
from 14 and 32 with the addition of SHQL coverage
([SHQL](../api/shql.md#testing)), the restart and eviction tests added with items 4 and 5, the
limit and cross-shard coverage added with item 7, the row-order and `IN`/`OR` coverage added
with items 26 and 39, the sort-key selection coverage added with item 8, the range coverage
added with [F1](../features/sort-key-ranges.md), the multi-log recovery and recovery
counting coverage added with items 31 and 9, the tablet map and storage marker coverage
added with items 11 and 12, the eviction accounting coverage added with item 13, the empty
rotated log coverage added with item 14, the compaction tail loss and marker format coverage
added with items 44 and 45, and the query error display coverage added with
[item 48](resolved/query-error-display.md). The counts before item 48 were 157, 194 and 11;
before F2 were 136, 183 and 11; before items 44 and 45
were 136, 178 and 11; before
item 14 were 135, 178 and 11;
before item 13 were 135, 177 and 11; before items 11 and 12 were 133, 168 and 10; before
items 9 and 31 were 132, 159 and 10; before F1 they were 115, 129, and 8, and before item 8
were 105 and 116. [Resolved #126](resolved/storage-directory-unusable.md) added 2, both
`shoal-core` unit tests in `meta.rs`.

---

## High — data loss and silent failure

~~*Nothing is currently filed at this severity.*~~ ~~Item 124 is, filed at the end of this page
with the entries that came after the triage order.~~ ~~*Nothing is currently filed at this
severity* again:~~ ~~Items 260 and 261 are, below, both filed by a code review of F87 to F93.~~ ~~Item
261 is, below, filed by a code review of F87 to F93 with 260, which is
[resolved](resolved/checkpoint-in-segment-order.md).~~ ~~*Nothing is filed at this severity* again:~~ Item 285 is, below, filed when
[Tests That Fail Under Load](flaky-tests.md) was begun: a fixture row twice found every chunk of a
put not yet committed gone after reclamation passes, which loses an object if the put then
committed, and which reading is right is not established. Items 260 and 261, filed by a code
review of F87 to F93, are resolved
([260, 273](resolved/checkpoint-in-segment-order.md), [261](resolved/archive-driven-once.md)).
Item 124, the one before them, is [resolved](resolved/unsorted-update-generation.md).
Items 9 and 31 were the two before it and are both [resolved](resolved-issues.md).

~~One thread they shared is still worth pulling.~~ Both were about what happens when a copy read
from disk meets a copy already in memory: item 31's `scan` overwrote the in-memory copy, and
item 30 was the `Occupied` arm of the sorted table's `load_partition` throwing the disk copy
away without a word. Item 31 answered the question for recovery by removing the collision
entirely — nothing loads after a replay. **Item 30 closed the thread**, together with the two
defects found by reading its code: the unsorted table's arm replaced the resident copy and
charged the shard for both (item 120), and the collision was only reachable because a get did
not wait on a replicated apply's read already in flight (item 121). All three were reproduced
and fixed in one change ([Resolved #30, 120, 121](resolved/resident-copy-collision.md)). Both
tables now keep the resident copy, charge nothing, and log at `ERROR` if the two copies
disagree, and a partition has at most one read outstanding.

### 285. `discard_requires_a_committed_fact` finds a held put's chunks gone under load

`shoal/tests/cluster_fixture.rs` (`discard_requires_a_committed_fact`, *a put in flight lost a
chunk*, 27261); a put given up (`given_up` and `abandon`, `shoal-core/src/server/shard/objects/put.rs:597-640`)
and the reclaimer (`shard/objects/reclaim.rs`)

**High if it is what it looks like: a reclamation removed every chunk of a put not yet committed.**
The row holds a put before its commit (`HOLD_STAGER 8000`), waits until its three chunks are placed,
asks six reclamation passes 800 ms apart, and asserts all three are still held. Twice under load
none was (`left: 0, right: 3`). [F92](../features/reclamation-and-device-lifecycle.md)'s rule is that
nothing is removed but on the committed fact that frees it, and a put that then committed would
name chunks no device holds. The row's next assertion, that the held put was not refused, would
tell, but a run stops at the first.

There is a reading in which nothing is wrong. A put whose chunks were written but which failed
before its hold, as a stage answered late under load would fail it, is given up (`given_up`): its
id is retired, its chunks are reclaimed as they should be, and its client is told to put it again
under a new identity. The row's wait for three holders sees the chunks placed either way. A hold
that lapses during the passes is not such a reading: a commit after it makes the object current,
and one that fails answers the client without retiring the id.

**Established by running it**: in F97's workspace run and in the run of 2026-10-21, both at six
threads, each passing at three threads after ([Tests That Fail Under Load](flaky-tests.md)). Which reading is right is not.

**Fix direction:** have the row report the held put's answer and each pass's removals before it
asserts. A removal of the held put's chunks while its try is still registered is the defect; a try
given up before its hold is the load's.

**The row reports before it judges since 2026-10-27**: the held put's chunks counted after each
pass, the put's answer, and every node's reclaimer figures, when a chunk is gone.

---

## Medium — robustness

### 208. A write that fits a client frame can make a log entry no peer frame carries

A write is bounded by the client's frame, and the entry it becomes is bounded by nothing. The
server reads a request against `networking.max_frame_bytes` (64 MiB by default), proposes the
row as a `Command`, and the group's leader replicates that command inside an append framed
against the peer link's bound, which is the same setting. The command carries the row's archived
bytes behind its own heads (`COMMAND_HEAD_LEN`, `shoal-proto/src/shared/protocol/peer/replicate.rs`),
and the append carries the entry behind a log id, a length and the request's heads. So a row
that fills a client frame to within a few hundred bytes becomes an entry that no append can
frame, even alone. Nothing checks it at the proposal (`shoal-core/src/server/shard/groups.rs`,
`replication/admission.rs`). By reading, the leader appends it to its own log and can send it
to no follower, so it never reaches a quorum: the write is answered unknown at its timeout, and
every entry after it in that group waits behind it in every member's batch, so the group commits
nothing more for as long as that leader leads. A new leader never held the entry, and would
truncate it from the old one's log when it returns as a follower.

Since [Resolved #202](resolved/append-batch-bytes.md) a batch always carries one entry, so such
an entry is sent alone and refused alone; before it, it was refused inside a batch of three
hundred. Either way a member owed it is never fed past it.

**Established by reading the source**, while fixing item 202. It has not been reproduced. The
reproduction is a fixture test: a cluster at a 4 MiB frame, one write of a row a few bytes under
4 MiB, and the members' applied indexes.

**Fix direction:** refuse a write whose command would not fit an append at the proposal, by name,
before it is committed: the bound is the peer frame less the append's heads, known when the
group is built. A write refused that way is one the client could split, which is the same answer
a frame too large already gets.

**Since [F73](../features/bodies-across-frames.md)** a bundle longer than a client frame can be
streamed to a server that assembles it, which would put a row past a peer frame within reach of one
send. A cluster node therefore refuses `networking.max_request_body_bytes` above its
`max_frame_bytes` by name at start, so the reach of this item is unchanged: a row a few bytes under
the frame, as before.

### 209. A bench event arm's `converged` mark reads the wrong node's lag

A `stop` or `kill` arm of `shoaladm bench` ([F67](../features/dataset-benchmarks.md)) starts its
victim again and marks the moment its groups caught up, and that mark measures something else.
`catch_up` (`shoaladm/src/bench/events.rs`) polls the cluster model through the bench's admin
client and waits for the victim to be `up` and for `model.lag_max` to settle. `lag_max` is the
`replication` section of the status the admin's own node reports
(`shoaladm/src/cluster/model.rs`), which is that node's largest committed-to-applied gap
(`ShardReplication::lag_max`, `shoal-core/src/server/replication/report.rs`). It says nothing
about the node that was stopped, and a follower that has not been sent entries has no
committed-to-applied gap of its own either. So an arm marks `converged` about a second after the
restart whether or not the victim caught up.

**Reproduced** on the lab while checking [Resolved #202](resolved/append-batch-bytes.md): an
insert-only arm of 256 KiB rows with titan stopped from 10% to 60% of it, on a build where titan
cannot catch up at all, marked `converged at 37.7s` against a restart at 36.7s. The same build
left titan with 51.6 MiB archived against 506 MiB and 997 MiB on the other members, minutes
later.

**Fix direction:** judge catch-up by the victim's own figures against its peers', which
`STATS` already carries for every member: its applied index per group against the leaders',
or, short of an index, its archived partitions and WAL segments settling level with the
others'. A member's own `lag_max` is the wrong question in both places it is asked.

### 211. A bundle refused at its send loses its queries from the bench's record

A query the driver failed to send is meant to be sent again or recorded as failed, and a whole
bundle refused at its send is neither. `flush` (`shoal-loadgen/src/driver.rs`) moves the
worker's `buffer` into the bundle it sends, and empties `staged` only once the send succeeds.
When the send fails, the worker's error path pairs what is still staged with what is still
buffered, `staged.drain(..).zip(buffer.drain(..))`, to queue each query again or record it
failed. The buffer is already empty, so the pairing yields nothing, and every query of the
refused bundle is dropped: not ok, not failed, not retried. The consequences:

- An insert dropped this way was never acknowledged, so the verify pass does not count it lost.
- A preload that drops rows reports itself complete with fewer rows than the file holds.
- An arm after such a preload counts the missing keys as misses, and the run finishes with no
  error.

**Reproduced** while fixing item 210. On the tree before that fix, a preload bundle of sixty-four
rows refused with `PayloadTooLarge` left a preload of 36 rows out of 100, with nothing in the
capture's errors ([Resolved #210](resolved/bench-preload-frame.md#evidence)). Item 210 stops the
bench choosing such a bundle, but not this loss. `send` writes to the connection itself
(`ShoalQueryStream::send`, `shoal-client/src/client.rs`), so a connection that breaks while a
bundle is written takes the same path, as in a `kill` or `stop` arm. By reading, that bundle's
queries are dropped the same way, which makes an event arm's failure counts low. That half has
not been reproduced.

**Fix direction:** take each staged query from its own copy (`Staged::copy`, kept whenever
retries are allowed) rather than from the buffer, and record an insert with no copy as failed.
Record a refusal that sending again cannot cure, such as `PayloadTooLarge`, as failed rather
than retried: a query that was never sent keeps its attempt count, so it would be retried for
ever.

### 214. A bench feed keeps 4,096 rows ahead whatever their size

A table's insert feed parses rows ahead of the workers and holds up to `FEED_AHEAD` of them,
4,096 (`shoal-loadgen/src/feed.rs`), counted in rows and bounded by nothing else. A dataset of
rows of 1 MiB therefore holds up to 4 GiB in the driver's process before a worker has taken one,
and one of 4 MiB rows 16 GiB. On the lab the driver shares europa with a node of 8 GiB, so a
`shoaladm bench run` of wide rows can take the host's memory from its own cluster, or be killed
by the kernel before its first arm. The preload streams through the same feed.

A second bound beside it judges a run by the file's text rather than by its rows. The frame
check and the preload's bundle are sized by the file's mean row in bytes of text
(`TableScan::mean_row_bytes`, with `FRAME_HEADROOM` of four, `shoaladm/src/bench/orchestrate.rs`).
That is conservative for any honest encoding of bytes, which is larger as text than as a row,
and wrong for a row whose deserializer expands a short description into its bytes: the check
passes, the preload goes back to bundles of sixty-four, and the refused rows are lost as
[item 211](#211-a-bundle-refused-at-its-send-loses-its-queries-from-the-benchs-record) says.

**Established by reading the source**, while choosing how to drive
[X3](../object-storage/spikes.md#x3-bytes-through-the-tablet-groups), whose rows are 64 KiB to
4 MiB. X3 did not use the bench for this and other reasons
([X3's record](../object-storage/bytes-through-groups.md#the-harness)), so neither half has been
reproduced.

**Fix direction:** bound the feed in bytes as well as rows, by the parsed row's own size, which
`DeepSizeOf` gives every table's row; and size the frame check and the preload's bundle by the
largest row the scan parsed, measured the same way, rather than by the file's text.

### 215. A replication lane busy with wide rows is judged silent, and refuses forwarded writes

A write sent to a member that does not lead its group is hopped to the leader over the
replication lane (`ShardPeer::propose`, `shoal-core/src/server/replication/network.rs`), and the
hop is refused at once, `NotLeader`, when the lane "has answered nothing" for the hop's silence
bound. A hop already sent is given up on the same way, `OutcomeUnknown`. The bound is
`HOP_SILENCE`, raised to three heartbeats of the failover base, 1.5 s at the default, and silence
is the time since a request outstanding on the link was last answered (`silent_for`). Both came
from [Resolved #143](resolved/silent-partition-hops.md), where a peer that answered nothing was
cut off. A lane carrying wide rows over a full 1 GbE link is not cut off and still answers
nothing for that long. It refuses writes the leader would have taken.

**Reproduced by [X3](../object-storage/bytes-through-groups.md)** on the lab's three hosts at a
factor of three, with rows written by six streams spread over every member, about 64 MiB in
flight, the busiest link at 90 to 93% of 1 GbE. Every refusal but 23 shed at admission named the
lane's silence:
`NotLeader` with "… has answered nothing on the replication lane for 1.545s; the write was not
sent" or "the lease of … lapsed: no quorum acknowledged it within 5s", and `OutcomeUnknown` with
"the peer has answered nothing on the replication lane for 1.50s; the request was sent and may yet
land". By size, over four rounds:

| Rows | Preload sent again | Puts and overwrites refused | Small writes beside them refused |
| --- | --- | --- | --- |
| 64 KiB | none | none | none |
| 256 KiB | 21,246 to 25,467 times a round, 98,304 rows | 7,867: 5,409 `OutcomeUnknown`, 2,435 `NotLeader`, 23 `Shedding` | 75 |
| 1 MiB | 2,571 to 4,133 times, 24,576 rows | 1,207 | 13 |
| 4 MiB | 5 to 67 times, 6,144 rows | 19 | 1 |

A refusal that had in fact landed was sent again and stored twice: at 1 MiB the WAL writer wrote
1.11 to 1.17 bytes for each byte the preload stored, against 1.00 at the sizes that met no
refusal. Before the preload was made to send a row again until it was written, one round's
preload at 256 KiB gave up on 3,471 rows after eight tries each. The same rows on three nodes
over loopback, at 165 to 290 MiB/s and with nothing between them but loopback, were refused
nothing in any round. Which queue a hop's answer waited in, and why 256 KiB most, are **not
established**. By reading, an answer comes back on a link that also carries that peer's appends
to this node, and for the bytes in flight the appends of 256 KiB rows are the most entries of any
size that met a refusal.

**Fix direction:** judge a lane silent by what it received, not by what it answered. A link
that has received bytes on the replication lane within the bound is not cut off, which is the
only case #143's refusal is for. And keep a forwarded write's answer from queueing behind a
peer's appends, the way the client wire keeps small answers apart
([F73](../features/bodies-across-frames.md)).

**In part since [Resolved #284](resolved/a-burst-starves-its-replication.md)** (2026-10-27): a member's
requests unanswered past the hop silence count as its silence only while the member sends nothing for
as long, by member, so a leader whose appends reach a follower is waited on. That is the first half
of the direction above. The lab case has not been run since, and this item stays open until it is.

### 216. Writes faster than a node's merges hold it past its memory budget

A node's budget (`node_memory`, divided among its shards since
[Resolved #149](resolved/node-memory-budget.md)) is kept by eviction, and eviction takes only
partitions on the shard's LRU. A partition holding a write no archive has yet is not evictable
(`shoal-core/src/server/tables/persistent/unsorted.rs`, where every write takes its partition off
the LRU) until the merge of the segment the write was logged in marks it evictable again
(`mark_evictable`). So the rows a node holds are its budget plus every row written since its
merges last caught up, and nothing bounds the second term but the merge rate. A node written to
faster than it merges grows past its budget for as long as the writes go on, and back under it
once its merges catch up.

**Reproduced by [X3](../object-storage/bytes-through-groups.md)** on europa: one node at a
factor of one with a budget of 6 GiB, written rows of 256 KiB to 4 MiB at 560 to 970 MiB/s. Its
resident set reached 23.0 GiB in one 40 s put arm at 256 KiB, with 22.1 GiB of rows held as the
shards count them. A put or overwrite arm, or a preload, went past the budget in thirteen of its
sixteen cells, at 7.3 to 23.0 GiB. Its gets and mixtures, which write half as fast or not at all,
kept to the budget, and so did every arm of every other leg, since none wrote faster than its
nodes merged:
three nodes on the same host at a factor of three each took about 220 MiB/s, and the lab's 1 GbE
held its nodes to about 60. The settle after each arm brought the node back under its budget.
On a host with less memory than europa's 43 GiB, such a node would meet the kernel's OOM killer
before its budget.

**Fix direction:** hold a write's admission, not only a read's partitions, to the budget. A
shard over its budget with nothing evictable is behind its merges, and can refuse or slow writes
until they catch up, the way a group's `pending_bytes` bounds what is proposed and not yet
answered.

### 338. A node below its resident floor evicts its rows at every check

A node past `node_memory` has every shard evict forty percent of its rows at each check
(`shoal-core/src/server/shard.rs`, `over_memory`, `Resident`), on the process's resident set, and a
node's resident set holds about 1.4 GiB that no figure of F104's names: its rows, its WAL's index
and its archive maps came to 0.2 to 0.4 GiB of a node's 1.6 to 2.0 GiB at rest. So a node whose
`node_memory` is below that floor is past its budget for good: every check evicts its rows, the
rows it evicted are read back from disk, and nothing it evicts can bring it under.

**Observed by [X16](../future-work/scans/walk-beside-the-foreground.md#what-was-found-on-the-way)**
on the lab (2026-10-29), a side cluster of the spike's schema on europa (6 shards) and the two Zen1
hosts (5 each), three copies of a kibibyte row table and two others. At `node_memory` 1 GiB every
node sat at 1.56 to 1.77 GiB resident with 23 to 115 MiB of rows, and each arm that read the
resident table evicted 42,000 to 56,000 partitions a member, which put the foreground's reads on
disk. At 4 GiB, the same cluster held 1.58 to 2.02 GiB with 95 to 234 MiB of rows and evicted
nothing. Not reproduced in a test: the floor's makeup is not known, and is the first thing to find.

**Fix direction:** a heap profile of a node at rest (`tmdb-dataset --features jemalloc-prof`) to
name the floor; then either count it in a node's figures and refuse at start a `node_memory` below
it, or judge the pressure on the memory the shards' rows hold rather than the process's whole
resident set.

### 340. An archive file the map no longer knows is never deleted

`shoal-core/src/server/tables/storage/fs/compactor.rs`, `compact_archives`;
`shoal-core/src/server/tables/storage/fs/map.rs`, `replay`

An archive pass logs an archive's `DeleteArchive`, makes the log durable, takes the archive out of
the map, settles the map, and only then unlinks the file. If settling the map commits a manifest and
begins its log again, and a crash comes before the unlink, the restart finds a manifest without the
archive and a log that never names it: the file stays on disk, and no pass ever deletes it, since a
pass weighs only the archives the map knows. [Item 339](resolved/archives-known-after-restart.md)'s
archives are the same kind of file: before its fix, an archive unknown after a restart that later
died was never deleted, and a cluster that ran under that build keeps such files after the fix,
since the log entries that named them were folded into a commit and no replay names them again.
Their bytes stay on the device; nothing reads them wrongly.

Found by reading the source while item 339 was fixed (2026-10-30). Not reproduced: the crash's
window is between a map commit inside `settle_map` and the unlinks after it.

**Fix direction:** a reconciliation of each table's archive directory against every shard's map at
a node's start, every file no map knows and no entry names removed and counted; it covers both
kinds, and has to run across the node's shards, which share the directory.

### 221. A strong exists takes no read barrier, and is never refused as stale or steered off an installing copy

`shoal-core/src/server/shard/reads.rs`, `await_read_barrier`; `shoal-core/src/server/shard/migrate.rs`,
`stale_tablet`; `shoal-core/src/server/shard/snapshots.rs`, `installing_group`

A read at `Quorum` waits for a barrier from each of its groups' leaders and then for its replica
to apply through it ([F41](../features/read-consistency.md)). The groups are found from the
query's partitions, `query.partition_keys()`, and that method names only a get's: it is "the
order the rows merge in", and every other kind - an exists included - names none
(`shoal-proto/src/shared/queries/{sorted,unsorted}.rs`). So an exists asked at `Quorum` collects
no group, waits on no barrier, and is answered from whatever its replica has applied: a strong
read served as a read at `One`. The same empty list is what `stale_tablet` checks for a tablet no
group here serves and `installing_group` checks for a copy installing a snapshot, so an exists on
a retired copy is answered from the files the cluster no longer counts rather than refused
`StaleTopology`, and one on an installing copy is answered from it rather than steered elsewhere.

**Established by reading the source**, while [F74](../features/client-routing.md) gave every query
kind `route_keys`, which names an exists's partitions and a write's as well as a get's. Not
reproduced.

**What it costs:** an exists at `Quorum` can say no for a row a committed write put there, on a
follower that has not applied it; and on a retired or installing copy, at any level.

**Fix direction:** use `route_keys()` in those three places, where the question is which tablets
a query touches rather than in which order its rows merge. Write the test first: a `Quorum`
exists through a follower must count a barrier in the fixture's `GATHERS`, and one through a node
whose copy retired must be refused `StaleTopology`.

### 225. A load that had every row acknowledged never finished

`examples/tmdb_dataset/src/load.rs`, `worker`, the drain after `queries_tx.close()`; the client's
query stream end (`shoal-client`)

**Medium: a client stalled for good against a cluster that was serving.** On the lab, a full TMDB
load started about half a minute after the `tmdb` cluster restarted, while titan handed group leads
back, had every one of its 3,382,336 rows acknowledged within 55 seconds - its progress counts
only rows answered `Ok`, and its five ten-second figures sum to the dataset - then printed `0
rows/s` for ten minutes and never reached its read-back. The process was alive and idle on 33
threads; no node logged a warning or an error after the hand-back's, every member was up, and the
same load run again at once finished in 37 seconds. Three `NotLeader` retries were sampled during
it, for one group titan led.

~~**Observed once, at `4f3246d`, and not reproduced.**~~ ~~**Observed twice**~~ ~~**Observed three times**~~ **Observed four times**: at `4f3246d`, and at
`33d1cbb` on 2026-10-11, when [F80](../features/object-wire.md) redeployed the tmdb cluster. That
load also started within a minute of a bootstrap, while the leads were handed to their preferred
leaders. It wrote nearly every row (Movie held 1,157,912 partitions, the cluster's insert rate fell
to zero) and then printed `0 rows/s` for ten minutes until it was interrupted; run again at once,
it finished in 42.7 s. No client trace was taken, so the second sighting places it no better than
the first. The third was at [F82](../features/root-capacity.md#performance)'s tree on 2026-10-12, a
load started about two minutes after a bootstrap, with elections in its first seconds. Its five
ten-second figures sum to the dataset (3,382,350 rows, against 3,382,336, to their rounding), and
then it printed `0 rows/s` for ten minutes. Before it was stopped, its threads and sockets were
read: 32 runtime threads parked, and 42 connections, all to titan and hyperion, each having
exchanged only its hello (213 bytes out, at most 1,619 in), with nothing queued in either
direction and none left to europa. So no connection owed it an answer: **what it waited on was in
the client's own process.** Run again at once, it finished in 38.0 s. The fourth was at [F88](../features/recovery-and-moves.md)'s commit
`b5a29b2` on 2026-10-14, a load started about a minute after the tmdb cluster was bootstrapped
fresh: it read 1,100,000 movies in 35 seconds, the cluster's insert rate fell to a trickle and then
to nothing with Movie at 1,158,883 partitions of the csv's 1,187,693 ids, and it printed `0 rows/s`
for two and a half minutes until it was stopped. No retry of any kind was sampled, no node logged a
warning, and its 66 connections had nothing queued either way. Run again at once, it finished in
33.3 s. After the first, two more loads started fifteen seconds after a
restart, through more churn - elections, `OutcomeUnknown` and `Shedding` retries - finished, as had
seven smaller loads that evening. A worker leaves its loop once nothing is in flight or waiting to
retry and its jobs are drained, then closes its query stream and reads to the stream's end; with
every row answered the drain is the likely place it waited, but **where it stopped is not
established**: nothing was captured from the client before it was killed. Reading finds nothing of
[F77](../features/authorization.md), whose lab work met it, on this path: the load ran as the
admin, whose bundles skip the authorization check.

**Fix direction:** reproduce with the client's tracing at `DEBUG`, and give the loader's drain a
deadline that names what it is still owed, so a stall says where it is rather than printing zeros.

### 233. A snapshot install holds its stream twice, and the reserve check counts it once

`shoal-core/src/server/shard/snapshots.rs`, `begin_snapshot` and `cleanup_install`;
`shoal-proto/src/shared/capacity.rs`, `RootCapacity::need`

**Medium: a disk the reserve check judged roomy can fill during an install.** A stream is assembled
into a partial file under the node's primary root, and a persistent table's install copies every
record of it into the table's archives (`CompactionJob::Install`). The partial is removed by
`cleanup_install`, and only once a checkpoint that carries the install has landed
(`checkpoint_carried_installs`). So until then a node holds the stream's bytes twice: once as the
partial and once in the archives. Where the primary and the archive root are one filesystem, which
is every node with one root, the install needs about twice the stream above the reserve. The
receiver asks for it once (`need`: the stream plus the reserve, on each root), and so does the
planner. A stream that fits once and not twice is accepted, and the second copy eats into the
reserve the check was meant to keep. A node whose archives are on a root of their own needs the
stream once on each, which is what [F82](../features/root-capacity.md)'s check asks.

**Established by reading the source**, while F82 made the check judge every root. No test fills a
disk to the margin.

**Fix direction:** count a move's bytes once for every use it lands under on the same device. Group
a node's roots by `device`, and charge a device the stream for the primary and again for a
throughput root on it. Or remove the partial once the compactor's install job is written, if the
checkpoint can be shown not to need it. Either changes what the planner blocks on every one-root
node, so it wants the lab's figures for how near the reserve a real move runs.

### 241. A sorted partition written before it was read is digested without its archived rows

`shoal-core/src/server/tables/persistent/sorted.rs`, `PersistentSortedTable::digest` and
`PersistentSortedTable::canonical_cut`

**Medium: a scrub of a persistent sorted table can judge sound copies divergent.** A write to a
sorted partition that is not resident starts a partition in memory holding only its new rows,
with `check_disk` set, since its archived rows have not been read (`insert`'s vacant arm,
`SortedPartition::new`). A get reads the archive and merges before it answers. The digest and the
scrub's canonical cut do not: each takes a resident partition as the whole of it, hashing only
its new rows, and leaves its key out of the archived half. Two copies holding the same rows hash
differently whenever one holds such a partition and the other has merged or evicted it, so a
`Repair` in verify mode, or a scheduled scrub (`scrub_interval`, off by default), can quarantine
a sound copy. On the lab the tmdb cluster's `MovieByKeyword` partitions are in that state after a
load: F87's walk found 29,002 of them resident on every member, each merged over its archive.

**Established by reproduction, for the digest.** `a_partly_read_sorted_partition_is_digested_whole`
(`shoal/tests/tablet_walk.rs`, ignored until this is fixed) writes sixty groups of four rows,
restarts a node so every group is archived and evicted, adds one row to each of twenty groups,
and asks every shard's digest. The walk counts the 260 rows the table holds; the digest counted
180:

```text
assertion `left == right` failed: the digest left out the archived rows of partly read partitions
  left: 180
 right: 260
```

**Established by reading the source, for the canonical cut**, which takes the same resident
partition the same way (`canonical_cut`'s resident pass hashes `partition.rows` and adds the key to
`resident_keys`, which the archived half skips). No fixture test scrubs a persistent sorted table:
the fixture's schema has none.

**Fix direction:** fold an overlay over its archived rows wherever a partition is read whole, as
F87's walk does (`PersistentSortedTable::walk_fold`): the digest reads the archived record of a
`check_disk` partition and merges, and the canonical cut collects such a key's archived chain
beside its resident rows and merges them off the loop. The walk's fold is the shape to share. A
fixture schema with a persistent sorted table is what would hold the scrub to it.

### 244. A load that saturates a 1 GbE link delays the control lane's reports, and members fall silent

`shoal-core/src/server/control/detector.rs`, the phi-accrual detector the control leader runs over
each member's status reports; `peer::link`, every lane of a peer over the same link

**Medium: a member taken for down while it serves.** On the lab, `shoaladm bench`'s object arms at
eight 1 MiB operations in flight keep the 1 GbE links between the hosts near their rate, and the
control leader called members silent through every kind of arm on 2026-10-14: hyperion 13 times in
one minute of the steady arm, when no device had failed and nothing moved, and 12 to 14 times in a
minute of the device arms (`journalctl -u shoal-f88three-bench | grep "fell silent"`, at `b5a29b2`
and `402c1bc`). Each verdict was committed and cleared within a second or so. A member called down
is not placed on, so a placement group's move onto one of its devices is given up and tried again
([Resolved #243](resolved/move-slots-on-the-map.md#still-open)), and its groups' leads may be
handed elsewhere. The control lane's frames share the link with object bytes, and a status report
queued behind a link's worth of chunks arrives late, which the detector reads as silence.

**Established by reading the logs against the arms' timing**; not reproduced in the fixture, whose
nodes share one host's loopback.

**At two in flight it is rarer, and still costs moves.** F88's figures were taken at two 1 MiB
operations in flight, about 20 MiB/s each way, at `1220ff0` on 2026-10-15
([F88](../features/recovery-and-moves.md#on-the-lab)). No member was called silent in a steady
arm. In the three runs of a device's failure there were seven verdicts, all while the moves ran:
three in the first, two of them both other members at once four seconds after the event (phi 8.7
and 12.2), a leader's own lag, which the fix direction's last clause is for; one in the second; and
titan three times in the third, twice in its first fifteen seconds. titan was where every moving
group was arriving, and the 24 moves onto it then in flight were given up and begun again. The
additions, whose event restarts titan, had two or three verdicts a run.

**Fix direction:** keep the control lane's reports ahead of object bytes - a priority the kernel
honours (`SO_PRIORITY` with a qdisc that uses it), or object lanes paced below the link - and judge
silence against the leader's own lag, so a leader whose loop or link fell behind does not condemn
every member at once.

### 247. A failed device's pool gathered stale positions after its moves settled

`shoal-core/src/server/shard/objects/recover.rs`, the rebuild task; `put.rs`, a put's straggler
wait; the background gate in `devices/mod.rs`; a device's free space

**Medium: stripes left a copy short for as long as the load lasts.** In one run of
`shoaladm bench`'s `device_fail` arm on a copy of the lab's three nodes, `rw50` at 1 MiB, two in
flight, at `baf6335`, titan's `/xfs` was failed and its placement groups moved off; the moves
settled with 915 positions stale, and the stale count then rose while the rebuilds all but stopped
(`pools`, every fifteen seconds):

```text
01:46:43 target 6  moves 0  behind 0  stale 915   rebuilt 997  switched 2715
01:47:59 target 6  moves 0  behind 0  stale 1016  rebuilt 1028 switched 2715
01:49:14 target 6  moves 0  behind 0  stale 1254  rebuilt 1043 switched 2715
```

The arm then waited for no stale position, and was stopped; the bench's rule became a pool's
moves settling ([F88](../features/recovery-and-moves.md#operations)).

**Its last device filled.** The pool's usable space, which `pools` prints on the same line, fell
from 107 GiB when titan's `/xfs` failed to 5.5 MiB at 01:49:14, the poll at which the stale count
jumped to 1,254. Every titan position was then on titan's `/xfs2`, a 40 GiB volume made for F88's
figures. Nothing is reclaimed before M20, and every 1 MiB put writes a whole 4 MiB chunk
([O107](optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)),
so under `rw50` the device filled ten minutes after the failure. A full device refuses a stage, so
every put marks titan's position missed and nothing can be rebuilt onto it, for as long as the load
goes on. A run at eight in flight at `1220ff0` met the same sooner: titan's `/xfs2` ran 97 to 99%
busy from the failure, the pool's usable space fell from 106 GiB to 10.4 GiB in five minutes, the
device stopped writing at 03:18:56, and from 03:19:02 no position was rebuilt while the stale
count rose from 1,244 to 2,212 ([F88](../features/recovery-and-moves.md#eight-in-flight)). F88's
figures at two in flight ended each failure within five minutes, before the device filled, with 0,
1 and 1 stale, and a failure under a light foreground, 256 MiB objects read one at a time, with none.

**What a full device does not explain.** In the run at `baf6335` the rebuilds slowed before the
device was full: from about 3.5 a second to under one from 01:43, with the pool's usable space
still 65 GiB, while the stale count rose from 721 to 1,087. Two candidates: puts marking titan's
position missed whenever its one device is slower than the straggler wait
([O112](optimizations.md#o112-a-put-under-load-marks-its-slow-holder-missed-and-hands-recovery-a-rebuild)),
and the rebuilds onto that device starved of idle time by the gate; titan's `/xfs2` was 69% busy
over that run. The rebuild task logs a failed rebuild at DEBUG only, and the run was at INFO.

**Established by reading the poll and the device counters** against the arms' timing: the full
device is established, and the slowing before it is not explained. Not reproduced in the fixture.

Since [F90](../features/erasure-coded-pools.md#on-the-lab), a third candidate: a placement group that
never held a stripe places at its pool's pin, on the failed device, and every put into it counts that
position missed, which nothing can rebuild while the device is failed
([item 250](resolved/empty-groups-move-without-a-slot.md)). ✅ Since that fix such a group moves off
the failed device at once, with no slot, so a put after its pass places elsewhere.

**Since [F92](../features/reclamation-and-device-lifecycle.md)** the slices a move leaves are released
and a replaced or deleted object's chunks reclaimed past their grace, which is the capacity this run's
device ran out of; the arm has not been run again since, so whether the stale count still rises, and
the slowing before the device filled, are open.

**Fix direction:** the full device is capacity, which M20's reclamation (✅ F92) and a chunk no longer than
its stripe (O107) give back; a planner by bytes and reserve
([todos](todos.md#what-f88-left-for-later)) would at least say that a failure domain left with one
small device cannot hold its share. For the rest, the recovery task's outcomes counted in `pools`
(done, put off for idle time, refused for space), so a run that climbs says which.

### 264. A corrupt length in an archive record kills shard zero's blocking thread, and its restore hangs

`shoal-core/src/server/objects/archive.rs`, `read_record` (528, the allocation at 542), `verify`
(555) and `ArchiveReader::open`

**Medium: one flipped bit in a backup hangs every archive restore on its node, where it should refuse
the file.** `read_record` bounds a record's entry by `MAX_ENTRY_BYTES` and allocates its object
straight from the record's 64-bit length (`vec![0u8; usize::try_from(length).unwrap_or(usize::MAX)]`),
compared with nothing. `verify` checks the file's length against its manifest first, so a truncated
file is refused by name, but the trailer and the manifest's checksum are read only after every
record, so a length corrupted in a file of the right size reaches the allocation:

- past `isize::MAX` it panics with a capacity overflow on shard zero's blocking pool, which has one
  thread (glommio's pool builder shrinks it to one) and catches nothing. The answer to `open` never
  comes, `driving_archives` stays taken, and no other archive on the node is driven. The dead thread
  held the pool's last receiver, so shard zero's next blocking call, any `spawn_blocking` or
  glommio's rename, remove, mkdir and truncate, panics on `failed to enqueue blocking operation`
  (read from the glommio fork, not reproduced);
- too large to map but under that, it aborts the node in the allocator;
- smaller, the read or its CRC fails and the file is refused, as it should be.

A valid archive holds each object whole in memory, in `verify` and again in `next`, outside the
assembly budget a client's put is held to (`ObjectsBase::task` holds none).

**Established by reading the source**, twice. Not reproduced.

**Fix direction:** ~~`read_record` is handed the bytes left before the trailer and refuses a length past
them before it allocates; a test flips a bit of a length.~~ Reading an object in pieces drawn from
the assembly budget answers the memory half, and is part of
[O117](optimizations.md#o117-an-archive-restore-reads-each-archive-two-or-three-times-and-copies-each-object-on-every-try).

**Its first half is fixed** ([Resolved #264, in part](resolved/archive-record-length-bound.md)):
every length a record names is judged against the bytes left before the trailer before anything is
allocated for it, and a corrupt one is the archive refused by name. **The second half stays open**:
a valid archive's objects are each held whole in memory, outside the assembly budget.

### 286. `an_erasure_coded_write_is_atomic_at_every_crash_point` finds a write with a member killed not acknowledged under load

`shoal/tests/cluster_fixture.rs` (`atomic_at_every_crash_point`, *the write was not acknowledged by
the two members left*, 25316)

**Medium: a write to a 3+3 pool with one node of three killed was not acknowledged.** The row's last
two rounds hold the coordinator's stager for 1.5 s, kill node one and then node two half a second
in, and assert the write is acknowledged by the members left, whose four positions are `k + f`.
Twice under load the round that killed node two was not. The atomicity the row is named for was not
reached, since it fails before the reads, and its replicated twin,
`stripe_write_is_atomic_at_every_crash_point`, has not failed this way in the record.

**Established by running it**: in F97's workspace run at six threads, and in the rotational run of
2026-10-22 (`SHOAL_TEST_ROTATIONAL=1`, the runbook's filter, at six threads), which passed at two
threads after ([Tests That Fail Under Load](flaky-tests.md)). Whether the write was refused, or answered after its 30 s, is not:
`write_at_through` reads both as not acknowledged.

**Fix direction:** keep the write's answer rather than a boolean, and print it when the assertion
fails. A refusal names its reason; an answer later than 30 s on a loaded host is the row's own
deadline.

**The row keeps the write's answer since 2026-10-27** (`write_at_answer`), and prints it when the
write is not acknowledged: a refusal names its reason, and 30 s is the row's own bound.

**Its other shape is fixed, and was the row's own** ([Resolved #286, in part](resolved/crash-row-arms-a-staged-holder.md)):
*stage_synced: none of nodes [1] died*, in F109's, F111's and `w10`'s runs, was a node armed to crash
at a stage its write never sent it. Node one's devices held only the object's two never-stored data
positions, one run in fifteen. The unacknowledged write stays open here.

### 288. `a_stalled_leader_hands_its_lead_on_first` found a write refused by a stalled leader's dead core

`shoal/tests/cluster_fixture.rs` (`a_stalled_leader_hands_its_lead_on_first`, 22392); the parked
batch's hold ([Resolved #167](resolved/stalled-leader-handoff.md))

**Medium: a write reached a stalled leader's stopped core, the refusal #167 ended, once under load.**
The row corrupts a leader's own record so that its apply stalls, writes the group's other keys forty
times through a follower, and asserts none is refused by a dead core. Since #167 a stalled leader's
parked batch is held by a task until another member leads, or for 3 s, and the lead goes to the
voter furthest along. In F95's workspace run one of the forty was refused `Unavailable` (*the shard
loop dropped an apply batch*), in the 4.2 s the forty took.

**Established by running it**, once, in F95's workspace run at six threads, which passed alone after
([Tests That Fail Under Load](flaky-tests.md)). Whether the batch was let go by a new leader or by the 3 s is not.

**Fix direction:** keep child logs for its next failure; the node's log says which let the batch go.
If it was the 3 s, an election under load outlasts the hold, and a write in between meets the dead
core as it did before #167.

**The refusal says which since 2026-10-27**: a stalled copy's batch is let go with its reason
(`ParkedApply::let_go`), which the stopped core's refusals carry: *its lead did not move within 3s*,
or *after its lead moved on*. The row prints the first such refusal, so its next failure says which
let the batch go, as the child logs would have.

### 296. A restore's table group stays at `Installing` under load, and its bucket archives wait behind it

`shoal-core/src/server/shard/restore.rs` (`drive_restores`, 883, which drives one group a shard at a
time; `drive_group_restore`, 299); `shoal/tests/cluster_fixture.rs`
(`a_restore_judges_each_table_by_its_own_identity`, 30803,
`objects_survive_a_backup_and_restore_by_archive`, 30907, and
`a_restore_never_puts_over_a_clients_write`, 31720)

**Medium: a restore never finishes under load, one of its groups making no progress.** ~~Both~~ The three
rows back up a table and a bucket and restore them into a cluster, the bucket's archives put again once
the table's groups are restored ([F93](../features/operations-and-the-real-cluster.md)). ~~Twice~~ Four
times under load the restore had not finished at the row's deadline, and the record it printed was the
same ~~both times~~ each time: every one of the bucket's three archives `Pending` with nothing put, two of the table's three
groups `Done`, and the third at `Installing` with no attempt counted. In F97's run that group still
named a driver. In F98's it named none, which is what a driver that lost the lead leaves when it
hands the group back (`drive_group_restore`). A group handed back is driven again by the shard that
leads it, at its next tick, unless that shard is driving another restore already: `driving_restores`
holds one at a time. So either no member led the group for the row's whole wait, or the one that did
was busy with another drive. [Item 142](#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s restore stall was a group left at its
first phase the same way, one path of which [Resolved #183](resolved/restore-driver-uncommitted-done.md)
closed.

**Established by running it**: in F97's workspace run and in F98's, each at six threads and each
passing at three threads after ([Tests That Fail Under Load](flaky-tests.md)). In the workspace run
of [Resolved #304](resolved/lone-frame-loss-probe.md) as kept (2026-10-24) two rows failed it so,
`objects_survive_a_backup_and_restore_by_archive` and `a_restore_judges_each_table_by_its_own_identity`,
each group at `Installing` naming no driver, and both passed at three threads after. Which of the two
it was is not.

**Fix direction:** keep child logs for its next failure, whose *driving a group's restore* and *left
for its new leader* events say which shard held the group and when; and have the rows print each
group's leader beside the record when they give up.

**The rows say who led each group not done since 2026-10-27**: when a backup or restore wait gives
up, each live node's view of every group not done (its shard, the leader it names, whether it
leads, whether its copy installs, its applied index) follows the record.

### 305. A capture's 1 MiB puts across hosts stalled whole, and its check could not read 10 objects

`shoaladm bench` on the lab's SSD leg at 2+1. The node's put path, where is not known

**Medium: a cluster that took no put for minutes, and said nothing.** It happened in
`w2-ssd-ec21-fix`'s third capture (2026-10-23). That build (e99379f1) is item 304's fix as first built,
a minimum RTO of 5 ms on every connection, with F107's cache by default. Its first six arms ran as the
others' did, and then:
- **The 1 MiB puts at one in flight** completed 7 in their first second, and none after, through the
  measured window and its drain.
- **The 1 MiB puts at sixteen in flight** completed about 230 in their first four seconds, and none
  after, through 276 seconds of drain.
- **The run's check** read 236 of 246 acknowledged objects back, none of them lost or wrong. 10 failed
  `ConnectionLost`.
- **No node logged anything at WARN** until the cluster was torn down, when every peer link dropped.

The same arms in the build's other capture, in F107's capture beside it and in lab 2's six captures on
F104's to F106's builds ran as before.

**Established by the lab's capture** (`shoal-spike/results/stores-ab/w2-ssd-ec21-fix/`, its third
capture's two put runs left out by the report as measuring nothing). Not reproduced in the next
eight captures of the same arms in the same order (2026-10-23): that build's twice, F107's four
times and the scoped fix's twice (`w2-ssd-ec21-stall`, `w2-ssd-ec21-scoped`), every run measured
and every check reading 2,048 objects back. Nor in four more on 2026-10-24, F107's twice and the
fix as kept twice (`w2-ssd-ec21-client`). Nor in nine more of the fix as kept that day, under three
settings of the hosts' TCP: O131's diagnosis and its follow-up (`w3-ssd-ec21-o131`,
`w3-ssd-ec21-o131e`), whose arms include the same 1 MiB puts at one and at sixteen in flight.

**Not reproduced on its own build either (lab 3b, 2026-10-25).** That build, e99379f1, ran with
[F108](../features/object-work-in-flight.md) laid on it so its work in flight could be read
(`a64c8ef6`, never merged). It took six captures in a row of lab 3b's SSD arms (`w3-ssd-ec21-i305`),
puts and gets of 64 KiB and 1 MiB at one and sixteen in flight, with every host's sockets and TCP
counters sampled each two seconds beside them (`stores-sample.sh`).
- Every one of its 96 runs measured, and every check read 2,048 objects back.
- No run's lanes refused a request, no budget shed a draw, no shard refused a task and no put was
  given up. The most any run held was 27 object tasks and 23 requests waiting.

Nor in lab 3b's 31 captures after them, 746 runs of F109 to F112 on the SSDs and on loopback, every
one measured and read back whole. Nor in lab 3c's four (2026-10-25, `w3-ssd-ec21-f113`), 64 runs of
F111 and F113 on the SSDs, every one measured and read back whole: no lane refused a request, no
budget shed a draw, no shard refused a task and no put was given up, and the most any run held was
24 object tasks and 24 requests waiting. Nor in the SSD leg of the comparison's second run
(2026-10-25 and 26, 257592b3): 416 runs, every check reading its objects back and every run measured
but one, whose 1 MiB get waited on an object lane for an answer until the run ended. F108's figures
say where that one waited, and it is filed as
[item 316](#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound)
in case it is not this item. Nor in the disk leg (2026-10-26): 416 runs, every check reading its
objects back and every run measured but one, the same arm's get, which waited the same way and is
item 316 again.

**Not an upgrade's restart.** The capture ran from 21:13 to 21:23 UTC, hours from any host's
unattended upgrade, which each host runs between 06:05 and 06:45. No upgrade has restarted a lab or
store unit except the one [item 306](resolved/lab-run-restarted-over-its-cluster.md) filed.

**Fix direction:** reproduce it, with the comparison's arms in their order, on the builds around it:
item 304's first fix, its scoped fix and F107's. Then read a node's object executors' budgets, its
lanes' queues and its stages in flight while it stalls. Since
[F108](../features/object-work-in-flight.md) a node's figures carry them, a capture keeps them
(`stores-ab-report.py --objects`, its unmeasured runs included), and the refusals that end such a
wait log a WARN.

**Its silent waits, bounded.** Reading the put path for this item found four places a put or a read
waited with no bound and nothing logged, each fixed under its own number: a served lane whose writer
stopped ([Resolved #307](resolved/a-lane-whose-writer-stops-ends.md)), a client's attempt
([#308](resolved/an-object-attempt-gives-up.md)), one put's full feed stopping its connection's reader
([#309](resolved/a-full-feed-leaves-its-connection-read.md)), and connections that never probed a
vanished peer ([#310](resolved/peer-connections-keepalive.md)). Each fits this stall, and none is shown
to be its cause. Item 311, a stream task left waiting, is filed beside them.

### 316. A get's request on an object lane went unanswered, and its coordinator waited with no bound

`shoaladm bench` on the lab's SSD and disk legs at 2+1, on 257592b3. ~~Where the request was lost is
not known: the asking link (`objects/lane.rs`, `Lanes::send` and `on_event`), the holder's served
lane, or between them~~ The request or its answer was held above TCP, at one end of its lane, until
the next traffic on that lane. Which end is not known, nor whether the kernel's TLS layer or the
program held it

**Medium: a get that waited ~~until its run ended, with nothing logged~~ ten seconds for an answer
that did not come, until its read was refused and went to another holder**
([Resolved #316, in part](resolved/unanswered-read-bound.md)). It happened ~~once, in the
fourth SSD capture of the comparison's second run (`shoal-ec21-drift`, 2026-10-26):~~ twice, in the
same arm on two legs of the comparison's second run, each time late in a 2+1 capture:
- **On the SSD leg** (`shoal-ec21-drift`, 2026-10-26), a 1 MiB get at one in flight, started at
  02:42:19.7 UTC in the third of `get100/s1m/c1`'s four runs, was never answered. Nothing else
  completed for the rest of the run, 25 measured seconds and a 10 second drain, so the run measured
  407 gets where its others measured about 2,400. The client gave it up with `ConnectionLost`
  ("early eof") once its 30 s retry budget was spent, its server having closed the connection, most
  likely as
  [Resolved #317](resolved/owed-connection-kept.md) describes.
- **On the disk leg** (`shoal-ec21`, 2026-10-26), in the same arm's fourth run, a get started at
  08:10:42 UTC and nothing completed after it, so the run measured 445 gets where its others measured
  about 1,800. It ended `Timeout` 60 s later, at the bound
  [item 308](resolved/an-object-attempt-gives-up.md) gave an attempt that makes no progress, its
  retry budget spent by then.
- **Each node's work in flight says where it waited**, the same both times. The capture keeps it
  every two seconds since [F108](../features/object-work-in-flight.md). From then until the run
  ended, europa ran one object task with one request waiting on one of its object links, while no
  node sent or served a request and no served lane on titan or hyperion owed an answer. A link holds
  a request's place until an answer arrives under its id or the link drops, and a served lane counts
  a request owed from its reading until its answer is written or dropped. So the holder ~~never read
  the request, dropped it unanswered, or answered it under an id the link did not hold~~ had not read
  the request, or had written its answer and europa had not read it.
- **On the disk leg it was answered late, not lost.** Europa still held the request's place and its
  task at the start of the next arm, the same gets at sixteen in flight, and had given both back by
  the start of the arm after it, 38 s later, with no line logged by any node between. A request's
  place is given back only by its answer, by its link dropping, or with the link itself when its
  executor stops or its holder's address changes. So its answer came a minute or more late, while
  the next arm's gets went over the same links, and that arm ran as its other runs did, 123 gets a
  second. On the SSD leg the next arm made 133 a second, none slower than 492 ms. 15 s into it
  (02:43:10.99), hyperion's object link to europa timed out and europa's to hyperion was reset, which
  would have given the request back too. Lanes do that about ten times a capture in the arms that
  fill a link.
- **Nothing was held in TCP.** Every peer lane, dialled or accepted, aborts a connection whose data
  goes unacknowledged for 5 s (`unacked_timeout`). On the disk leg no lane or link dropped from the
  run's start to the next arm's end, so neither the request nor its answer sat unacknowledged. Each
  was either never handed to TCP, or delivered and acknowledged. What waited was above TCP, at one
  end:
  - a frame in the asking link's queue that its writer was not woken for;
  - a record the kernel's TLS layer did not send;
  - bytes that arrived, but whose reader (the kernel's TLS layer or the executor) was not woken.

  The next frame on the same connection moves any of them.
- **Nothing was logged** but the SSD leg's drop.
- ~~**Only since europa's kernel moved, so far.**~~ **In this arm, only since europa's kernel moved.**
  Every capture since 2026-10-21 ran this arm 255 times
  with europa on 7.0.0-34, and none failed. Europa has run 7.0.0-38 since 2026-10-25, and two of the
  24 runs across hosts since then are these. The kernel is not the only difference: those earlier
  captures ran ten arms or fewer, where the comparison cycles through 26. Two in 24 cannot tell the
  two causes apart. The loop leg's eight runs of the arm, every node on europa's 7.0.0-38 and no
  link between them, all finished, and none of its 224 runs stalled. The run below added 192 more
  of the arm with europa on 7.0.0-38 (2026-10-27), and none stalled: two of 216 on that kernel.
- **But stalls of the same look ran on 7.0.0-34 too** (found 2026-10-27). ~~In F101's draws
  (`f101-hdd-rep3-draws-m4-14`, 2026-10-21, build 21466829, every host on 7.0.0-34), all sixteen
  gets in flight of `get100/s64k/c16`'s first run stopped together 30 s in, on copies on disks. Its
  drain stayed idle for 320 s, and the run ended with `ConnectionLost` sixteen times
  (`target/lab/f96/chain-f101-again.log`). That was before F108 could say where it waited, and all
  sixteen at once is another shape than one read.~~ The stall in F101's draws, all sixteen gets in
  flight at once on copies on disks, is not this item. F101's third run met that shape again, and
  F108's figures put it on the coordinator's own disk, with no lane in it. It is
  [item 326](#326-every-get-one-node-coordinated-stopped-held-on-a-read-of-its-own-disk-and-nothing-was-logged).
  [Item 305](#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects)'s
  stall was on 7.0.0-34. What it and this item's two share: each crossed hosts, every peer lane
  ran under kernel TLS, and none was on the loop leg.

**Established by the lab's captures** (`shoal-ec21-drift` on the SSD leg and `shoal-ec21` on the
disk leg, slimmed into `shoal-spike/results/stores/`), their F108 figures and the three nodes'
journals. ~~It is the only one of the 288 runs above the inline threshold in the comparison's four
SSD captures~~ They are the only two of the 576 runs above the inline threshold in the comparison's
eight SSD and disk captures with a request waiting through two samples while no lane moved, and none
of lab 3b's and 3c's 906 such runs had one.

**Related to item 305**, a stall nobody heard about on another build. This may be its class, seen
now that F108 counts what waits.

**Fix direction:** ~~read the three places a request can be lost on a link that goes on working: a
frame the asking link was handed and never wrote, a request a served lane read and dropped
unanswered (every chunk request is answered once), and an answer under an id its link holds no
request for, which `on_event` drops without a word.~~ find the end that held it. Run the arm, and
read every node's sockets the moment a run stalls: both ends of each lane (`ss`'s queues and
counters), and the asking link's queue and the frames it wrote. Run it again with plaintext lanes
([F103](../features/plaintext-peer-lanes.md)), which leave the kernel's TLS layer out. ~~Then bound a
coordinator's wait on a holder's answer, as item 308 bounded a client's attempt, so a held request
is refused by name and the read goes to another copy or decodes.~~

**Its bound is fixed** ([Resolved #316, in part](resolved/unanswered-read-bound.md)): a reader's read
over an object lane is refused by name once its answer has not come within
`cluster.transport.object_read_timeout`, ten seconds, and goes to another holder or decodes. The
refusal reads the asking end's half of what the fix direction asks for, at the moment it happens: at
WARN, whether the link wrote the read, the answers the link read since, and its socket's queues each
way, and since 2026-10-27 whether the kernel had anything for the link to read (`readable`, a
zero-timeout `poll`), which under kernel TLS sees a record its layer holds where the queue count does
not. A late answer is logged ~~at INFO~~ at WARN with how late it was, since the lab's nodes log at
WARN. **Which end held it, and why, stays open.** The holder's end is read by nothing in the node;
its served lane's figures said it owed nothing. The next lab run is planned
([below](#the-run-that-reads-both-ends)).

#### The run that reads both ends

The arm alone on the SSD leg at 2+1, `get100/s1m/c1` with 24 runs a capture, eight captures
alternating plaintext peer lanes ([F103](../features/plaintext-peer-lanes.md)) with the default of
kernel TLS: 96 runs a side, about 2.3 hours, the user's choice of 2026-10-27. Beside it a watcher
fires on three idle seconds in a capture's log and snapshots every host's sockets (`ss -tinpmoe`,
with their `tcp-ulp-tls` state), `/proc/net/tls_stat`, the node's threads and its F108 figures,
three times a second apart, inside the ten seconds before the bound refuses the read. A refusal's
`written`, `readable`, `unread` and `last_received_ms` against `waited_ms` name the end at the
asker; the two ends' sockets of the lane, matched by port, say where the bytes were; and the sides
say whether kernel TLS takes part. If neither side stalls, the comparison's 26 arms come next.

**It ran on 2026-10-27** (`i316-ssd-ec21`, `bde2927b`, slimmed under
`shoal-spike/results/stores-ab/i316-ssd-ec21/`), and **neither side stalled.**
- 192 runs, 96 a side, and none had an idle second. The watcher never fired.
- Every capture read its 1,024 objects back.
- Gets a second were 80.0 [74.5–84.1] over kernel TLS and 85.9 [77.3–91.6] over plaintext lanes,
  1.07 and within. The p99 was 20.1 and 19.9 ms.

So the arm alone did not make it in 192 tries, where the comparison met it twice in 24, and this
run neither shows nor clears kernel TLS. The comparison's 26 arms, which cycle the lanes through
puts, patches and gets of every size, would be the next run, which is the user's call. The disk leg's stall in
F101's third run, which ran beside this one, is
[item 326](#326-every-get-one-node-coordinated-stopped-held-on-a-read-of-its-own-disk-and-nothing-was-logged).

### 132. `ephemeral_sorted_table` aborted once in glibc's thread-cache teardown

One workspace run aborted this test binary with SIGABRT after its first test passed. glibc
printed `tcache_thread_shutdown(): unaligned tcache chunk detected`. That message means a heap
chunk on a thread's cache list was corrupted before the thread exited: something wrote past an
allocation or freed one twice. The other fourteen tests in the binary never reported.

**Established by running it, once.** The run was `cargo test --workspace --no-fail-fast --
--test-threads 6` for [F55](../features/cluster-upgrade.md), a change that touches no engine
code. Run alone straight after, the binary passed all fifteen tests. The cause is not known.
The suspects are the code that is `unsafe` about a heap it does not own: the zero-copy archived
responses, and the `Send` wrappers this page lists under **Unsafe `Send` invariant**. The next
step is running the binary in a loop under ASan (`RUSTFLAGS=-Zsanitizer=address` on nightly)
until it aborts, then reading the report's first frame.

**A candidate cause was found and fixed, and is not shown to be this.**
[Resolved #133](resolved/read-plan-rc-across-shards.md) was a shared `Rc` in every split query's
metadata, raced on by two shard threads: heap corruption from exactly the `unsafe` `Send` this
entry suspected. A standalone server splits gets across shards too. But this binary, run under
ASan (`-Zsanitizer=address -Zsanitizer-recover=address`) on the unfixed tree, passed all fifteen
tests and reported no use-after-free, only gxhash reading past the end of a 13-byte key, which it
does on purpose within a page. So this stays open until it recurs on a tree with #133 fixed, or
never does.

**Looped under ASan in round 12, 169 runs, no abort.** The binary built with
`-Zsanitizer=address -Zsanitizer-recover=address` on `030b57a` (the tree with
[Resolved #152](resolved/kanal-receive-races.md)) ran all fifteen tests 169 times at six threads
on europa, beside lab loads (`target/lab/r12/132/loop.sh`). Every run reported exactly one error,
at one address: gxhash's `get_partial_unsafe` reading past a short key. Nothing else, and no
`tcache` abort. It has not recurred in any suite run since it was filed either, so it stays filed
with that as its record.

### 142. Two fixture tests fail intermittently on an idle host

`lost_response_retry_returns_original_result` stops at `group …'s checkpoint never reached 5:
[(0, Some(0))]`: node zero, killed and restarted, catches up (its digest matches) but reports
checkpoint 0 for the group for the whole 60 second wait, through repeated `ROTATE` and `COMPACT`
verbs. `migration_resumes_after_each_phase_failure` fails less often.

**Established by running it**, on the development host with nothing else running, at the commit
that fixed items 139 to 141 and applied O62:

| Test | Current tree | `5c49888` (before 139–141, O62) |
| --- | --- | --- |
| `lost_response_retry_returns_original_result` | 4 of 17 runs failed | 0 of 6 |
| `migration_resumes_after_each_phase_failure` | 2 of 8 runs failed | 0 of 3 |

Restoring the old fixed map fold on the current tree gave 8 of 8 passes, but the next 8 runs with
O62 on passed too, and the lost-response test had also failed once in an earlier loaded run from
before these changes. With logs on (`SHOAL_CHILD_LOG`), 6 of 6 runs passed. The samples are too
small to say whether the changes made the failure more likely, and nothing found links them. What
decides a restarted member's checkpoint is that a sealed WAL segment is handed to the compactor
only once every group on the shard has applied past it. A group on node zero that has not applied
past a segment would hold every other group's checkpoint on that shard. Filed with its rates so
that the next change to that path, or the next run, can say more.

**Round 15** ([#142 in round 15](../cluster-testing/correctness.md#142-in-round-15)): the whole
workspace at six threads passed 1,740 of 1,740, and the loop of the six heaviest tests found two
shapes in 66 runs. One was [#195](resolved/scheduled-scrub-starved.md), a node's scheduled scrubs
starved by version churn, fixed; 60 runs on the fix passed. The other, once:
`migration_resumes_after_each_phase_failure` with one group's move left at `Configured` for 240 s
after its driver was killed at that phase, its `activate` never answered. Its logs were not kept;
the loop now keeps them.

**Round 16** ([#142 in round 16](../cluster-testing/correctness.md#142-in-round-16)): the whole
workspace at six threads passed 1,745 of 1,745, and the loop ran again with every failing round's
logs kept (`target/lab/r16/142/loop-keep.sh`). Its first round caught the stuck move round 15
saw once, with its logs: **[Resolved #197](resolved/retired-driver-holds-slot.md)**. A move's
source that won the election a killed driver left resumed at `reconfiguring`, skipped the
hand-off that keeps a source from driving its own removal, and reconfigured itself out; the
published map then retired its copy under the running driver, whose lead check read the
shut-down handle's frozen metrics, so it polled a dead group for the migration timeout holding
the shard's one driver slot - and the next move the shard led, at `Retiring` after *its* driver
was killed, was never driven. Fixed, with a fixture test that fails on the unfixed tree. One
panic in the round was not this item and is not to be filed here:
`a_restarted_volatile_leader_elects_nobody_missing_its_commits` hit openraft's
`log_state_reader.rs:25` assertion once, on the first run of the fixture's failover tests on
[F62](../features/failover-window.md), because the amnesiac grace had halved with the lease;
the grace is four bases again and the test passed four of four after it. The workspace run on
the round's last code passed 1,744 of 1,746, with two deadlines under load that passed alone
twice each and are this item's: `single_node_data_has_a_verified_cluster_migration_path` judged
its restore before the records landed (*the groups restored other records than the export holds:
0 against 400*), and `unplaced_member_forwards_every_query` read a node's readiness before the
placement was initialized (*initialized: false* with every member up). Six rounds of the loop on
#197's fix passed 36 of 36. The first recurred in the workspace run for
[F65](../features/query-figures-home-tab.md), at the same line with *267 against 400*, and
passed alone in 8.8 s. That is its third failure in a full run, so it belongs in the loop's
list.

The suite run for [Resolved #144](resolved/post-heal-elections.md) at six threads failed
`lost_response_retry_returns_original_result` again, and also `scheduled_scrub_quarantines_without_an_operator`,
which had not failed before. Both passed alone afterwards: two and six runs out of two and six.
The run for [O67](optimizations.md#o67-ten-thousand-retained-entries-is-seconds-of-a-busy-group)
failed the lost-response test again, with `local_rehome_recovers_after_each_crash_point` and
`returning_node_catches_up_by_log_or_snapshot` (which sets its own retention, so the new default
does not reach it). All three passed alone straight after. What the loaded failures share is a
deadline the suite's load outruns, which is what this item is now tracking.
The run for [#146](resolved/apply-wait-on-a-lagging-copy.md) failed
`returning_node_catches_up_by_log_or_snapshot` a second time, and this one was the test's own
race, now fixed: *"the senders counted 6 transfers and node two installed 7"*. A receiver counts
an install when it lands, and its sender counts the transfer only when the end's answer reaches
it, so a read of both in between finds one more install than transfers. The test now waits up
to ten seconds for the senders' count.
The run for [#159](resolved/map-ahead-of-archive.md) failed
`scheduled_scrub_quarantines_without_an_operator` and `local_rehome_recovers_after_each_crash_point`
again, and both passed alone, three runs each.
The first suite run after [#155](resolved/restore-retries-unreachable.md) failed its new test,
`a_restore_rides_out_an_unreachable_member`, which passes alone (ten of ten, four at the time and
six more after the disk-full rerun on the lab) and passed in the next full suite run. That run's log kept only the summary line, so the failure's message was not
captured. The test waits out a 25 s isolation with a 20 s phase deadline and a 5 s retry pause, and
the margins are what a loaded host erodes.
While proving [#170](resolved/uncached-log-reads.md), `migration_resumes_after_each_phase_failure`
was run eight times on its own on the fixed tree: 2 of 8 failed. One failure was the test's
`"no member of the target is up to take the lead"`, and one was openraft's own debug assertion in
the test process's node, `Some(log_id) <= committed` at `log_state_reader.rs:25`: a member was
handed a log id at an index it had already committed, under a different leader. Because #170
changed how a leader reads its log, the same loop was run on `0296da8`, the tree before it. The
assertion fired there on the first run, and 2 of the 3 runs taken failed. So it predates #170,
and it is the first time this item has a message from inside openraft rather than a deadline.
The assertion is the one [Resolved #109](resolved/volatile-majority-loss.md) met for a volatile
log. Whether this test's durable groups can reach it the same way is not established.
The `shoal-core` change for [#148](resolved/stale-intent-log-tail.md) was followed by one failure
in the first four runs of `shoal/tests/persistent_unsorted_table.rs` at six threads, whose name the
run did not keep, and none in the next nine.
The seven full runs of the suite for items 160 to 168 at six threads gave these, each run with
nothing else on the host:
- `local_rehome_recovers_after_each_crash_point` and `migration_resumes_after_each_phase_failure`
  failed once each (the rehome test on *"No such file or directory"* from a child). Both passed
  alone. The rehome test then failed one of five runs alone, on *"node two never died at
  before_finalize"*, the first failure of it alone on record.
- `a_restore_rides_out_an_unreachable_member` failed once, with one group handed back at
  `Pending` for want of the isolated member and not driven again within 300 s. It passed alone,
  and in the next two full runs.
- `lost_response_retry_returns_original_result` failed once, on its usual checkpoint.

Two failures in those runs were not this item and were fixed: `mixed_versions_exchange_real_cluster_operations`
raced its activation against a member report (on [Resolved #155](resolved/restore-retry.md)), and
`an_unreadable_partition_stalls_one_copy_and_repairs_it` did not retry a write the stalled copy
refused retriably.
The full run for [Resolved #176](resolved/unreadable-voter-log.md) at six threads passed 127 of 130:
`local_rehome_recovers_after_each_crash_point` (*"No such file or directory"* from a child again),
`scheduled_scrub_quarantines_without_an_operator` (its quarantine never committed in time), and
`a_move_asked_again_rebuilds_its_learner_from_nothing`, which is the test's own race and not this
item: its first move ended `Moved` before the test cut the spare's links, so the failure it waits
for never came. All three passed alone straight after.

**The openraft assertion is resolved** ([Resolved #142](resolved/volatile-amnesiac-vote.md)), and
it was not a timing. It was an acknowledged write lost. The group was the ephemeral table's, and
the leader the test restarts at the `reconfiguring` phase came back with its memory log gone. It
then granted its vote to a follower that had missed the last committed entry, and the follower
that kept it asserted when the new leader's entries reached it. The deadlines above remain open:
the checkpoint wait, the rehome's missing file, the scrub's quarantine and the restore's
isolation. After the fix, `down_retains_placement_during_grace` also failed once in a loaded run
of the fixture binary and passed three times of three alone.
[Resolved #152](resolved/kanal-receive-races.md) found that the compactor could lose a job it was
handed when its wait for the next job lost a race against a retry's timer. A lost merge holds its
WAL segment, and a held segment holds every group's checkpoint on its shard, which is the shape of
the lost-response test's *"checkpoint never reached 5"*. Whether it was the cause is not
established; round 12's suite runs record the rate on the fixed tree.
The first full run of round 12 (`05e9e15`, 1,715 tests at six threads) failed only
`a_restore_rides_out_an_unreachable_member`, its second failure on record, and in the same shape
as the first: one group left at `Pending` with no driver and no attempts, never driven again
within 300 s, while the other two were `Done`. It passed alone straight after, and ten runs of it
alone with twenty busy loops on the host all passed (`target/lab/r12/restore-loop.sh`). The record
says something the deadline alone does not: the only path that leaves a group at its starting
phase with no driver is a driver that lost its lead handing the group back for the new leader
(`drive_group_restore`), and every shard polls for a group it leads at every tick. So either no
member led the group for five minutes, or its leader's shard believed it was already driving a
restore (`driving_restores` is one at a time per shard, and cleared only by `RestoreDone`). The
next failure should be caught with `SHOAL_CHILD_LOG` to tell which.
Round 13 found a third way, and it is the one the record fits: the leader's shard believed the group
was **done**. A driver whose first progress commit failed also failed the group, and it reported
`Done` to its loop whether or not that `Failed` commit landed. When neither landed, the record
stayed at `Pending`, with no driver and no attempts, and the loop never drove the group again.
That is fixed as [Resolved #183](resolved/restore-driver-uncommitted-done.md), established by
reading the source. The stall itself was not caught: a loop of the test beside five other heavy
fixture tests at six threads, with child logs (`target/lab/r13/142/loop.sh`), passed it in every
round on the unfixed tree. It stays filed here until it is either caught or has gone a round of
suite runs without recurring.
Two of the rehome test's failures on record here, *"node two never died at before_finalize"* and
*"No such file or directory"* from a child, were one defect, found in round 13 with child logs and
fixed as [Resolved #186](resolved/marker-lost-update.md). The control thread rewrote the storage
marker while the pool rehomed the files, and a finished rehome's executor count was lost. They
are no longer deadlines. The lost-response test's *"checkpoint never reached 5"*, this item's
first and most frequent shape, was not a deadline either: a WAL segment recovered at a restart was
never sealed when a rotation reached the node before an entry did, and the sweep, which stops at
the first unsealed segment, handed nothing to a compactor again. It is fixed as
[Resolved #187](resolved/recovered-segment-never-sealed.md), caught with child logs in round 13.
What is left here are deadlines under the suite's load (a write not committed or a leader not
elected in time, the scrub's quarantine) and the restore stall, which has not recurred.
Round 14 found that most of those deadlines were a defect too. The six heavy tests run together
at six threads (`target/lab/r14/142/loop.sh`) failed 11 of 42 over seven rounds, five of the six
tests at least once, and the rehome test with its setup writes retrying for 30 s still failed on a
lease that lapsed for the whole 30 s. The leader's followers had handled every append; the leader
had thrown each answer away because it came later than openraft's budget of one heartbeat
interval, 100 ms at the fixture's base. That is [Resolved #190](resolved/append-answer-thrown-away.md):
an append now waits at least the election timeout. On the fix the same loop passed 30 of 30 over
five rounds. Two fixture changes went with it and are kept: a start waits up to 20 s for every
placed group's first leader (`Cluster::settle_leaders`), and the rehome, scrub, returning-node and
migration tests' setup writes retry, as does the lost-response test's acceptance of a commit that
outlasted its one second deadline (`OutcomeUnknown` beside `Timeout`). What is left here is the
restore stall, which has not recurred since #183, and whatever the next full suite run finds.
That run found a new shape: `leads_follow_the_members_lead_weights`, whose node weighted four
led 3 of 6 groups at the test's 120 s deadline. It passed alone four of four and failed two of
three beside the five heavy tests, and one of three with #190's floor taken out, so it is the
load and not the floor. ~~Under that load a node's links are judged impaired, and the balancer
moves no lead while they are ([#182](resolved/slow-link-leadership.md)).~~ It was neither (2026-10-28):
the row judged a draw. At 4:1:1 F58's rule names the node weighted four for each group with
probability two thirds, from identities every run mints afresh, so it is named for three or fewer
of six groups in a third of runs and four or fewer of nine in 14.5%, whatever the load. The
workspace run after [F64](../features/stats-tui.md) failed it once more the same way, with the
heavy node leading 2 of 6, and it passed alone in 21 s.

The workspace run after [X3](../object-storage/bytes-through-groups.md), at six threads, failed two
of 2,081, both of which passed alone. `single_node_data_has_a_verified_cluster_migration_path`
restored 267 of 400 records, the figure X1's run met. And
`a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver`, [#197](resolved/retired-driver-holds-slot.md)'s
test, found node two leading the group where it asserts the source won the election the killed
driver left: the test pauses node two so that the source's vote request is answered first when it
resumes, and under the suite's load node two's own campaign got there first. The first of these
is the deadline this item is about; the second is the test arranging an election by timing, and
it passed alone in 12 s.

The workspace run after [X8](../object-storage/small-writes.md), at six threads, failed one of
2,114: `single_node_data_has_a_verified_cluster_migration_path` again, restoring 150 of 400
records, and it passed alone in 8 s. The run after [X9](../object-storage/table-latency.md) failed
the same test the same way, 133 of 400, and it passed alone in 8 s.

Since 2026-10-22 every failure under load is recorded on
[Tests That Fail Under Load](flaky-tests.md), and ~~four~~ ~~three~~ ~~two~~ none of its rows are this
item's now:
~~`single_node_data_has_a_verified_cluster_migration_path`, which failed in fifteen runs from
2026-10-11 to 2026-10-21 with 0 to 267 of 400 records restored when it read them,~~
~~`leads_follow_the_members_lead_weights`~~, ~~`unplaced_member_forwards_every_query`~~ and
~~`a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver`~~. The lead weights row's child
logs in the flaky-test plan's loop had every group led by the voter the rule names, at [3, 1, 5], and
it judges each group's leader against the rule since 2026-10-28.
`unplaced_member_forwards_every_query` read readiness before its node's shards held a map, and reads
it once they do ([Resolved #280, #287, #290](resolved/fixture-reads-too-soon.md)). The page's first table is
what this item's deadlines turned out to be. **The migration row was never a deadline**
(2026-10-27): its records were a restore's report of a group finished by a drive that never
installed it, which says nothing was restored
([Resolved #327](resolved/restore-figures-kept.md)).

---

**`a_source_resuming_a_move_hands_the_lead_on_and_frees_its_driver` arranges its election since
2026-10-27**: it timed a pause so the move's source would win, and under load the paused member's
own timer won three times. A fixture verb now keeps that member from standing
([142, in part](resolved/an-election-arranged-not-timed.md)). ~~The item's other rows stay here, each on
[Tests That Fail Under Load](flaky-tests.md).~~ Its last, the lead weights row, left that page on
2026-10-28. What is left of this item is the restore stall, which has not recurred since #183.

### 311. A whole chunk's stream whose sender stops leaves its task waiting and its file held

`shoal-core/src/server/objects/serve.rs` (`Streams::take`, `Streams::write`, the `open` map)

**Medium: a leak, not a stall.** A whole chunk sent as pieces is one stream on its holder:
- the first piece starts a task, and its sender stays in the executor's `open` map, keyed by the
  slice, the connection it came on and the stream's nonce;
- the task writes each piece as it arrives, and ends at the last piece, at a failure, or when its
  sender is dropped;
- the sender is dropped only at the last piece or when a send to a task already gone fails.

So a stream whose asker stops sending never ends:
- a put given up between pieces, or an asker whose lane went down mid-stream, leaves its sender in
  the map and its task waiting on its next piece (`pieces.next()`);
- the task holds its `WholeWriter` meanwhile, a fresh chunk file of its class being written, or one
  taken from the slice's free pool;
- nothing drops a dead lane's streams, since a served lane's end leaves the `open` map as it was.

Every such stream holds a file and its descriptor until the node stops, and its chunk's class
loses a pooled file each time. It answers no request and holds no request's room, so it stalls
nothing.

**Established by reading the source**, while reading the object lane for
[item 305](#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects)'s
silent waits ([Resolved #307](resolved/a-lane-whose-writer-stops-ends.md) to
[#310](resolved/peer-connections-keepalive.md)). Not reproduced, and not counted anywhere: a node's
figures show no open streams ([F108](../features/object-work-in-flight.md)).

**Fix direction:** a served lane that ends drops every open stream of its connection, so each task
aborts its writer and gives its file back; and a stream with no piece for the lanes' stall bound is
given up the same way, for the streams of this node's own puts, whose origin is zero and never ends.
A test sends a stream's first piece and stops, then reads the slice's free pool back whole once
the lane is gone.

---

### 326. Every get one node coordinated stopped, held on a read of its own disk, and nothing was logged

`shoaladm bench` on the lab's disk leg at three copies, in F101's third run (`f101-hdd-rep3-i314`,
2026-10-27, `bde2927b`). The path is a coordinator's read of its own copy (`shard/objects/read.rs`,
through `ObjectStore` to the object executor that owns the slice), which no bound covers.

**Medium: every object get through one node stopped, unbounded and silent, for a minute or more.**
- **Where.** In the eighth of sixteen captures (`f101-hdd-rep3-i314-local-8`), the side with no
  margin, so every coordinator reads its own copy first. 64 KiB gets at sixteen in flight stopped
  together 28 seconds into the arm's second run, at 05:53:29 UTC. The arm after it, 1 MiB gets at
  one in flight, answered nothing in its whole run: its first get stopped at once.
- **Each node's work in flight says where they waited.** These are F108's figures, kept every two
  seconds.
  - From the stop on, titan ran sixteen object tasks, and seventeen once the next arm's get joined
    them.
  - Titan had one foreground request in flight on its slices and 256 KiB drawn from its object
    budget.
  - Titan had no request waiting on an object lane and owed none.
  - europa and hyperion had nothing in flight.

  So every get titan coordinated waited on titan's own copy, on its disk. At sixteen in flight each
  worker's next get through titan stopped within a second of the first.
- **Nothing was logged.** No node wrote a line from the capture's first run until its teardown.
  [Item 316](#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound)'s
  bound refuses only a read over an object lane. A read of a node's own slice that never answers is
  waited on for as long as its task lives.
- **It let go a run later.** Titan still held the tasks, the request and the draw when the 1 MiB
  run's drain ended, about 05:55:20. It held none of them at the next arm's first sample, three
  seconds later, and nothing was logged between.
- **The arm had stopped the same way before**, in F101's draws (`f101-hdd-rep3-draws-m4-14`,
  2026-10-21, build 21466829, every host on 7.0.0-34, a margin of four). All sixteen gets in
  flight of `get100/s64k/c16`'s first run stopped together 30 s in, on copies on disks. Its drain
  stayed idle for 320 s, and the run ended with `ConnectionLost` sixteen times
  (`target/lab/f96/chain-f101-again.log`). That was before F108 could say where it waited.
- **It is not item 316's shape.** There one read waited at its asking link, on a lane. Here no lane
  took part.

**Established by the lab's captures** and the three nodes' journals: once in the 128 runs of F101's
third run, and once in the 128 of its second. The slimmed captures are under
`shoal-spike/results/stores-ab/f101-hdd-rep3-i314/`. The full ones, with their F108 figures every
two seconds, are under `target/lab/stores/captures/hdd/`. Where the request stopped is not.

**Related to [Resolved #249](resolved/an-executor-parked-with-work.md).** ~~Its hangs in the
object lane's tests look the same on one host: a read on an object executor drew its budget and never
came back, its executor asleep. This may be that item's cause on a deployment, where no executor
shares a cpu.~~ That item's cause is found and fixed in the glommio fork: an executor that found a
preemption due before its queues ran went to sleep with their tasks queued. A deployed executor's
thread held past its 100 µs preempt timer meets it too. It does not explain this stall alone: an
executor asleep with work wakes at its next event, and this stall outlasted the next arm's first
get. Whether that get reached the executor is not known. The next disk leg, on a build with the
fix, says whether it comes again.

**Fix direction:**
- Catch it with the threads' states. Run the disk leg's 64 KiB gets at sixteen in flight beside a
  watcher that, on three idle seconds, reads every node's object executors' threads (`wchan`) and
  their F108 figures, and has the node log the stuck request's slice, chunk and await point.
- Bound a read of a node's own slice as item 316 bounded a lane's. It would be refused by name past
  `object_read_timeout`, with the slice's state logged, and sent to another copy.

### 329. A group of two voters elected no leader for a minute under load

`shoal-core/src/server/shard/groups.rs` (`start_group`, its head start; the shard's rule for standing);
`shoal/tests/cluster_fixture.rs` (`node_transfer_budgets_bound_concurrent_sources`, its setup)

**Medium if it is what it looks like: every write to a group's tablets refused for a minute.** The
row starts five nodes at a factor of two, so every group has two voters and needs both to elect,
and writes twenty batches of notes through node zero, each sent again until it lands. In the
confirmation loop's fifth round (`w6`, 2026-10-27) every try of one batch for a minute was refused
the same way, seventeen of them, and the row gave up:

```text
a note in the batch was refused: Server { …, code: NotLeader, msg: "group f3bef91496354dc5 elected no leader within the deadline" }
```

A fresh group is initialized by its placement primary, and its other voter stands after a head
start of four election timeouts, so a group whose two copies run and talk elects in seconds. A
minute without one is a copy that never started, never stands, or cannot reach the other: what
[item 142](#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s restore stalls and the
held copy of [its move resume row](resolved/an-election-arranged-not-timed.md) each turned out to be
in part. Which, here, is not known.

**Established by running it**, once, under item 328's fix. The row passed in the four rounds before
it and in every round of the loops before the fix.

**Fix direction:** read what the row now prints. Since 2026-10-27, each refused batch names its
group, and the row prints every node's view of it: whether each voter hosts a copy, whether the copy
is up, whom it names the leader, its log, and whether its shard stands for elections
(`ShardReplication::stands`).

**Seen again, with the views** (`w7`'s third round). Group `93faef223175298d` was held by nodes two
and three, its two voters. Each copy had **no membership** (`voters: []`), an empty log, no leader,
and its vote at term one. Node two's shard did not stand; node three's did. openraft counts a copy
initialized once its vote leaves term zero (`RaftState::is_initialized`), and refuses to initialize
it after that. So neither copy can ever be initialized, and with no membership neither can stand: the
group has no way to a leader. The head start's `initialize` and the primary's are each refused, and
were logged at DEBUG as *a group was initialized already*; since 2026-10-27 at INFO, as *a group's
initialize was refused* with the refusal. What moved both votes to term one with no
membership entry anywhere is not known: a vote is saved at once, and a log entry with the next batch's
sync. The next failure's child logs say, since the loop keeps them for this row alone
(`SHOAL_CHILD_LOG_TESTS`).

## Low — hygiene and documentation drift

### 300. The inventory wizard drops an inventory's `object_timings` when it edits one

`shoaladm/src/wizard/form.rs` (the inventory it builds, `object_timings: BTreeMap::new()`)

**Low: an inventory edited in the wizard and saved loses its `object_timings`.** The form keeps every
key of an inventory it edits that it has no field for, as it was: the removal grace,
`wal_commit_delay`, the `replication` block, and since [F103](../features/plaintext-peer-lanes.md)
`plaintext_lanes`. It builds `object_timings` empty instead. So the file it saves loses the
deployment's reclamation timings ([F92](../features/reclamation-and-device-lifecycle.md)) and its read
margin on disks ([F101](../features/disk-read-margin.md)), and a later `reconfigure` renders every
node at the engine's defaults.

**Established by reading the source**, while F103 carried its key through the same form
(2026-10-22). Not reproduced.

**Fix direction:** keep it as the form keeps `replication`: a field filled from the inventory it
edits and written back, with a test that an inventory round-trips through the form whole.

### 282. `an_erasure_coded_move_decodes_what_its_old_holder_lost` misses its deadline under load

`shoal/tests/cluster_fixture.rs` (`an_erasure_coded_move_decodes_what_its_old_holder_lost`)

**Low: a fixture row fails under the workspace's load and passes alone.** The row puts eight objects
of one 2+1 stripe each, loses node two's first device, fails it by an operator's order, and gives
every move ten minutes to decode the lost positions onto node two's second device. On 2026-10-20 it
failed twice under load: in F96's workspace run at six threads, before the loss, with *node two's
first device held nothing to move*; and in a rerun of that run's eight failures at three threads,
with *node two's second device holds 0 of the 3 positions its first held, 16 moves in flight* at
the deadline. It passed alone in 128 s, and beside two other rows of moves at three threads.

**Established by running it**, at F96's tree, without the cause. The failing runs kept no child
logs. The passing run beside the other two rows did: its moves waited their turn on the arrival
device (*has another move arriving on it*, a move a device at a time) and their groups' leaders
failed to confirm under load. None of F96's refusals is in them: no units past a file, no piece
refused, no `NoSpace`. F95's workspace run, at the commit before F96, passed the row; one pass is
not enough to clear F96, nor are two failures enough to blame it.

**The first message is a separate question**: whether eight objects' positions on node two can all
miss its first device by placement alone, about one run in 256 if each lands on either device
evenly, or whether the row read the device before the puts' chunks were in place.

**Fix direction:** run the row under the workspace's load at the commit before F96 and at F96 with
`SHOAL_CHILD_LOG`, a few times each, before anything else; give its deadline a figure from those
runs; and have it put until node two's first device holds a position, rather than assert that it
does.

### 279. shoal-bench's runner half no longer builds without the engine

`shoal-bench/src/fingerprint.rs` (181), `shoal-bench/src/run.rs` (122)

**Low: the half that judges a change does not build on its own.** `cargo check -p shoal-bench
--no-default-features` is the runner without `shoal`, which `compare`, `status` and `render` are
meant to need nothing more than, so a change can be judged while `shoal-core` will not compile
([bench runner](../features/bench-runner.md)). It fails with two errors: `fingerprint.rs` hashes a
file with `shoal::gxhash::gxhash64`, and `run.rs` names
`crate::workloads::harness::cluster::RemoteSpec`, both outside `#[cfg(feature = "workloads")]`. A
default build, which every workspace check is, compiles both, so nothing noticed.

**Established by running it**, at `e440bb07`, by F94 while it added a renderer to that half.

**Fix direction:** gate both under `workloads`, with the fingerprint's hash taken from a crate the
runner already has, and add `cargo check -p shoal-bench --no-default-features` to the checks
CLAUDE.md lists, so it is run.

### 278. An inventory whose pool the engine refuses deploys, and every node restarts on it until a client gives up

`shoaladm/src/deploy/inventory.rs`, `Inventory::validate` (622), and the check it does not call,
`PoolSpec::check` in `shoal-proto/src/shared/pools.rs` (667, the refusal at 814)

**Low: a pool a node refuses is found by its nodes, not by the inventory.** An erasure coded pool
that names `small_writes`, as a copy of `tmdb_cluster.yaml` with its redundancy changed does, is
refused by every node at its start (`it names small writes, which an erasure coded pool does not
take`). shoaladm judges an inventory's shape, roots and devices before it deploys, and not its
pools, so a deploy or a bench installs the nodes, systemd restarts each every few seconds, and
`bench run` fails minutes later with `could not connect` and no word of why: the reason is in each
unit's journal.

**Established by running it**, by [F94](../features/bench-object-endpoints.md)'s first try of its
loop leg on the lab (2026-10-18): thirty-four restarts of each node before the bench gave up.

**Fix direction:** `Inventory::validate` checks each pool with the engine's own `PoolSpec::check`,
which shoaladm reaches through `shoal`'s client half, so an inventory is refused by name before any
host is touched.

### 275. `a_put_refused_by_a_moved_group_leaves_nothing_where_it_moved_from` outruns its wait for two drains' releases under load

`shoal/tests/cluster_fixture.rs`, the wait for both drained devices to empty (26817)

**Low: a test times out on a busy host, for a reason not established.** Two devices are drained at
once and the test waits up to 900 s for their releases to empty both. Under the workspace run of
2026-10-17, and again beside eight other fixture rows at three threads, they had not; alone it
passed in 296 s, and earlier that day beside two other drain rows at three threads in 412 s. Whether
the releases were slow under the load or one was stuck was not told apart.

**Established by running it**, failing twice and passing twice, for the workspace run of
[Resolved #260 to #273](resolved-issues.md). It failed again in F94's, F96's and 2026-10-21's workspace runs.
`a_put_through_a_stale_placement_leaves_nothing_on_a_drained_device` waits the same way, 300 s for
one drained device, and failed the same way (*the drained device emptied by its releases never
came*) in the sweep's run of [Resolved #253](resolved/put-left-on-moved-holder.md) and in the
rotational run of 2026-10-22, passing at three and at two threads after ([Tests That Fail Under Load](flaky-tests.md)).

**Fix direction:** when the wait gives up, report what each drained device still holds and which
groups still name it left, so a stuck release is told from a slow one; then lengthen the wait or
file the release.

**The rows say what each drained device still holds since 2026-10-27** (`wait_drained_empty`): its
chunks, the first few named, and every node's reclaimer figures, when the wait gives up.

### 254. A capture names the project's commit beside a node program the inventory names prebuilt

`shoaladm/src/bench/` (the capture's provenance), `tmdb_cluster.yaml` and every copy of it

**Low: a capture can describe a node it did not run.** An inventory that names `server:` hands
`shoaladm bench run` a program it installs as it is, and the capture records the project's commit,
the worktree it ran from, as `project.commit` beside the program's `program_sha256`. Nothing in the
capture says the program was built at another commit. The lab's inventories all name
`target/deploy/release/tmdb-dataset-node`, so a bench run from a worktree moved to a new commit
measures whatever that file was last built from.

**Established by running it** for [Resolved #253](resolved/put-left-on-moved-holder.md): a capture
labelled for `db1ae04` recorded `project.commit` `db1ae04c57fc` and `program_sha256` `eab42571…`, the
`5383d7a` build of 17:36 that day, and none of its nodes carried `db1ae04`'s sweep.

**Fix direction:** the capture records the program's modification time and a commit the program
names itself, built in by `shoaladm build`, and `bench run` refuses, or says by name, a prebuilt
program older than the project's commit.

### 252. Two of F91's refusal tests cannot pass under `SHOAL_TEST_ROTATIONAL=1`

`shoal/tests/devices.rs`, `rotational_device_needs_a_journal_device` and
`rotational_device_with_its_cache_on_is_refused`; `shoal/tests/utils.rs`, `for_this_run`

**Low: the runbook's rotational line fails by its own harness.** CLAUDE.md runs
`SHOAL_TEST_ROTATIONAL=1 cargo test -p shoal --test devices --test device_store --test objects`, the
M14 to M18 device suites again with every device a disk. Under it `for_this_run` gives every device a
test names a journal beside it, and the variable stands disk facts in for every device, so the two
tests that start a disk with no journal, or with its cache writing back, to see it refused are
started as a disk with a journal and refused for nothing:

```text
thread 'rotational_device_needs_a_journal_device' panicked at shoal/tests/devices.rs:214:13:
a start that breaks a disk's rule was not refused ("names no journal")
```

**Established by running it** for [F92](../features/reclamation-and-device-lifecycle.md), which
touches neither file; both pass in a default run. `device_store` and `objects` pass whole under the
variable, as do F92's six fixture rows.

**Fix direction:** both tests already arrange their own disk facts; they are the two that should not
take the run's rewrite, so they skip under the variable and say so, as a multi-slice test does.

### 19. `memory` has no serde default

`shoal-core/src/server/conf.rs:30-32` — the only field in `Resources` without a default. A
`resources:` block omitting `memory` fails to deserialize. Omitting the whole block yields
`memory: 0`, so eviction runs continuously.

### 21. Constant and comment mismatches

(`MIN_ARCHIVE_COMPACTABLE` and its two use sites said 100 MiB of a value that is 10 MiB, and were
corrected with [O74](optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench);
`default_intent_log_size` had the same mismatch and was corrected before it.)

Also `RemoteTracing::Grpc` exports over HTTP (`trace.rs:33-37`, `.with_http()`), and
`FileSystemThroughputWriterConf::write_behind` — a count of buffers, defaulting to 4 — is
deserialized with `deserialize_byte_size` (`.../fs/conf.rs:154-156`). Its latency-sensitive twin
one struct up is *not* (`:68-69`), so the two writers disagree about what the same field name
means.

### 22. Size accounting inconsistencies

- `UnsortedPartition::new` sets `size = row.deep_size_of() + 17`
  (`.../tables/partitions.rs:228`); `UnsortedPartition::update` sets
  `size = self.deep_size_of()` (`:313`). Different bases for the same field.
- `SortedPartition` tombstones subtract the row's bytes but the tombstone still occupies a
  `BTreeMap` slot, so delete-heavy partitions under-report.
- Sorted partition sizes are maintained by delta and never recomputed, so they drift.
- Recovery adds a partition to the counter in *archive bytes* and eviction takes it off in
  *deep size*. `FileSystem::load_scanned` (`.../storage/fs.rs:242`) does
  `*memory_usage.borrow_mut() += archive.len()`, which matches `MaybeLoaded::size()` while the
  entry is `Accessible`. Replay then converts it to `MaybeLoaded::Loaded`
  (`.../persistent/sorted.rs:336`, `.../persistent/unsorted.rs`) adding only the update's `diff`,
  so `size()` starts answering
  `partition.size()` — a `deep_size_of` — against an amount that was the archive extent's length.
  `evict` subtracts the new base. The residual per partition is `read.len() - partition.size()`,
  either sign. This is the most concrete candidate for the `drift` that
  [item 13](resolved/eviction-log-underflow.md) now reports, because it is the one place two
  different size *bases* meet on the same counter rather than two different arithmetic paths.
  Note [item 31](resolved/multi-log-recovery.md) shrank this considerably without meaning to —
  the old `scan` re-added `read.len()` once per update intent, so a partition with *N* updates
  was counted *N* times.

The fix for the first bullet is the one filed as
[O4](optimizations.md#o4-deep_size_of-is-a-recursive-walk-called-on-every-mutation), ranked **B2**
there. The mismatched bases exist *because* the size is re-derived at each call site instead of
being owned by one, so carrying a row's measured size alongside it settles this item and removes a
recursive walk from every mutation with the same edit. That is why O4 outranks the other write-path
entries despite the profile saying the write path is waiting on the device — it is bought for a
correctness fix, and the cost removal is change left over.

### 23. Client stream and pool rough edges

- `ShoalResultStream::skip(0)` panics: `skip -= 1` precedes the zero check
  (`shoal-core/src/client.rs:1001-1008`).
- `is_valid` / `has_broken` use `peer_addr()`, which does not probe the peer (still marked TODO).
  [F11](../features/error-channel.md) narrowed this rather than closing it: both now also refuse a
  connection whose *read half* has stopped, which is the case they were silently passing. A peer
  that is gone but whose socket has not been reset still looks healthy, and that needs a `Ping`.
  **[F16](../features/client-builder.md) narrowed it again from the other side** — a client now
  has other endpoints to fall back to when one stops answering — without closing it either, since
  detecting that it stopped is still what is missing.
- `Shoal::send` archives the bundle before `track_response` may regenerate its id
  (`shoal-core/src/client.rs:209-215`).
- Two large commented-out blocks remain (`shoal-core/src/client.rs:549-603`, `:1048-1114`).
- `suceeded` / `QuerySuceededOpts` are misspelled in the public API.

**Fix direction:** the health-check half is [D6](../direction/connection-pool.md#health-checks-that-work),
which needs [D2](../direction/framing.md#message-types)'s `Ping` — a discriminant the wire format
has and nothing is behind. The rest are local edits and need nothing.

### 24. shoalctl warnings

`TabState::next` / `prev` are dead code (`shoalctl/src/components/tab.rs:548`, `:558` — the
compiler says so on every build), and `submit_query` `tokio::task::spawn`s the query only to
`.await` the join handle immediately (`:275-279`), so the UI blocks for the round trip anyway.

That `.await` ends `.unwrap()`, which is worth naming separately: a panic inside `query_bar::run`
comes back as a `JoinError` and is unwrapped on the UI task, which is the exact shape
[item 24's fixed half](resolved/shoalctl-panic.md) was about — a panic that unwinds past
`ratatui::restore()` and leaves the terminal in raw mode. The fixed half moved the *parse* error
off that path; a panic from the query round trip still takes it.

### 28. SHQL "Unknown field" names the field it is listing as valid

A field declared on a table but marked neither `partition`, `sort`, nor `filter` — an
`#[shoal(update)]`-only field, for instance — has no `FieldRole`, so using it in a `WHERE`
clause fails. The message it fails with is confusing:

```
Unknown field 'data'. Valid fields are: ["id", "title", "data"]
```

`data` appears on both sides. The cause is that `field_names()` is generated from *all* fields
while the `get_field_role` match arms are generated only for fields that have a role
(`shoal-derive/src/traits/table_schema.rs`), so the two lists disagree by construction.

The error is now raised in **one** place rather than two: the sorted and unsorted parse arms share
a `check_conditions` quote (`shoal-derive/src/structs/client.rs:238-247`). This entry used to name
`:127-137` and `:198-208` as the two sites, and that is worth recording because it makes the fix
smaller than the entry implies — a single generated block to change instead of a matched pair to
keep in step.

**Fix direction:** two different errors are being conflated. Distinguish them — if
`field_names()` contains the name, say `Field 'data' cannot be used in a WHERE clause because it
is not a partition key, sort key, or filter`; only say `Unknown field` when it is genuinely
absent. Better still, generate a `queryable_field_names()` alongside `field_names()` so the
"valid fields" list is the set that can actually appear in a `WHERE` clause.

### 29. SHQL rejects exponent notation with a misleading error

`float_number` is `(opt(sign), digit1, ".", digit1)`
(`shoal-core/src/shared/queries/parser.rs:524`), so digits are required on both sides of the
point and there is no exponent form. `.5` and `5.` fail with a reasonable
`Expected a value for field 'x'`, but `1e9` does something worse:

```
SELECT * FROM Movie WHERE id = 1e9
  =>  Unexpected trailing input: 'e9'
```

The integer parser matches the leading `1`, the condition completes, and the leftover `e9` is
reported by the end-of-input check as though the problem were somewhere else entirely. Confirmed
by running it.

**Fix direction:** low priority, since spelling the number out always works. If it is fixed, add
an optional exponent to `float_number` rather than special-casing the error — the error is only
misleading because the grammar accepts a prefix of what the user meant. Note that
`serde_json::Number::from_f64` already rejects the infinities a large exponent can produce, so
the overflow path is covered.

### 49. The coarsest parse errors report a span covering the whole query

Three of the errors a user hits most often blame the entire query rather than the part of it that
is wrong:

| Query | Reported span |
| --- | --- |
| `SELECT * FROM Nope WHERE id = 1` | `0..31` — all of it, for a table name in bytes 14 to 18 |
| `SELECT * FROM Movie WHERE title = 'a'` | `0..37` — all of it, for a missing partition key |
| `PICK * FROM Movie WHERE id = 1` | `0..30` — all of it, for a keyword in bytes 0 to 4 |

The first comes from the unknown-table arm the derive generates
(`shoal-derive/src/structs/client.rs:281`), which passes `0, query.len()`; the second from the
missing-partition-key arm beside it (`:507`), which does the same; the third from
`ParsedSelect::new` (`shoal-core/src/shared/queries/parser.rs:1312-1318`), which reports
`at_position(..., 0, query)` because the winnow parser it wraps returns a `ContextError`, and a
`ContextError` carries no input slice to recover an offset from.

Everywhere else the parser tracks real offsets — `field_start`/`field_end` on a `WhereClause`,
`start`/`end` on a `WhereValue` and a `ParsedProjection` — and computes them from
`original.len() - input.len()` on the surrounding `&str`. These three sites can do the same; the
`ContextError` is not the obstacle it looks like, because the offset comes from the input the
parser was handed rather than from the error it returned.

The cost is paid in shoalctl, which underlines the span an error names
([item 48](resolved/query-error-display.md)). A span covering the whole query is refused as
misleading, so exactly the errors a beginner hits first are the ones drawn without a mark. The
message names the position instead, which is correct and less useful.

**Fix direction:** for the unknown table, `ParsedSelect` already has the table name and could
carry its offsets alongside it the way `ParsedProjection` does. For the missing partition key,
the honest span is the whole `WHERE` clause rather than the whole query, since that is the clause
that has to change. For the failed `SELECT`, `query.len() - parsable.len()` at the point winnow
gave up is the offset, and it is already in scope — literally on the line above, as
`select_start` (`parser.rs:1310`), computed for the projection's sake and then not used for this.

### 40. `UnsortedExists` still names a single partition

`UnsortedGet` now carries `partition_keys: Vec<u64>` (`shared/queries/unsorted.rs:131`) and splits
across shards like its sorted counterpart ([26, 39](resolved/partition-order.md)), but
`UnsortedExists` (`:194-196`) was left with a scalar `partition_key`, and `split_by_shard` routes
an exists with a single `find_shard` (`:56`) where a get gets `group_by_shard` (`:50`).
`SortedExists` has taken a `Vec` all along, so the two table kinds now disagree about what an
exists can ask.

Nothing is wrong today: SHQL emits no exists queries, so the only way to reach one is the typed
API, where the single-partition shape is what the generated `*Exists` offers anyway. It is a
consistency gap that will bite whoever adds `EXISTS` to the query language, since the sorted
spelling will accept an `IN` list and the unsorted one will not.

**Fix direction:** the same change `UnsortedGet` took — a `Vec<u64>`, `for_partitions`, and
`group_by_shard` in `split_by_shard` — plus a `PendingGet`-shaped wait in the table so an exists
spanning partitions can park on more than one read.

### 41. SHQL cannot express a composite partition key

A table with several `#[shoal(partition)]` fields gets a tuple `PartitionKey`
(`shoal-derive/src/traits/partition_key.rs`), and the generated parse arm deserializes one
literal straight into that type. No SHQL literal is a tuple, so every query against such a table
fails with `Failed to deserialize partition key` however it is written.

The parser now guarantees one clause per field, which is what makes the fix tractable: the arm
could collect the conditions naming each partition field and build the tuple from them in
declaration order, so `WHERE a = 1 AND b = 2` would name one partition. Note that this is the one
place where `AND` across two fields is a conjunction *within* a key rather than a filter on top
of one, and that `IN` over a composite key would need each field's values crossed with the
others.

**Reachable since [Resolved #92, #198](resolved/composite-partition-key.md).** Until then a table
with a composite key did not compile, so this refusal had no table to be met on. It now has: a
schema can declare one and every typed query reaches it, and only the SHQL path refuses. The
refusal is pinned by `shql_refuses_a_composite_key` in `shoal-client-check`, which a fix to this
item turns into the test of what it accepts.

### 42. SHQL cannot express a composite sort key

The same defect as [41](#41-shql-cannot-express-a-composite-partition-key), one key over. Several
`#[shoal(sort)]` fields make a tuple `Sort` (`shoal-derive/src/structs/get.rs`), and the sorted
parse arm deserializes one literal straight into it, so `WHERE partition = 'x' AND a = 1 AND b = 2`
fails with `Failed to deserialize sort key` rather than naming a row.

Unlike the partition-key case the query still runs when the sort condition is simply left out — it
just returns the whole partition — so this is a missing capability rather than a table that cannot
be reached at all. It only started mattering with [item 8](resolved/sort-keys.md): while sort keys
were ignored there was nothing to express.

**Fix direction:** the same shape as 41, and worth doing in the same change. Collect the conditions
naming each sort field, build the tuple in declaration order, and reject a partial one. A prefix of
a composite sort key is a *range*, not a point, and although ranges exist now
([F1](../features/sort-key-ranges.md)) a range over a *prefix* still does not — lowering one needs
synthesized minimum and maximum values for the fields the prefix leaves out, which `Sort` does not
name ([TODOs](todos.md#sort-key-range-predicates--built)).


### 207. A sorted table with two `#[shoal(sort)]` fields does not compile

`shoal-proto/src/shared/traits/sorted.rs`, `type Sort`; `shoal-proto/src/shared/traits.rs`,
`impl RkyvSupport for String`

Several sort fields make a tuple `Sort`, the same way several partition fields make a tuple
`PartitionKey`. `Sort` is bound by `RkyvSupport`, which is implemented for `String` and nothing
else, so the derive's expansion fails for any table with a second sort field:

```text
error[E0277]: the trait bound `(std::string::String, std::string::String): RkyvSupport` is not satisfied
 --> shoal/tests/zz_scratch_sort.rs:5:57
  |
5 | #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf)]
  |                                                         ^^^^^^^^^^^^^^^^ the trait `RkyvSupport` is not implemented for `(std::string::String, std::string::String)`
```

[Item 42](#42-shql-cannot-express-a-composite-sort-key) describes a composite sort key as a SHQL
gap, which assumes the table compiles; it does not. Nor does a single sort field of any type but
`String`. [F54](../features/tmdb-dataset-deployment.md) met this and joined its title and id
into one padded string, and the bench schema pads its `Event` sort key the same way. Neither was
filed as a defect.

**Established by reproducing it**, with a throwaway test binary written while fixing the
partition key's version of the same shape
([Resolved #92, #198](resolved/composite-partition-key.md)), which this is not part of: a sort
key is ordered as well as hashed, and what order a tuple of fields has on disk, and how a range
over a prefix of it is lowered (item 42, [TODOs](todos.md#sort-key-range-predicates--built)), is
a design question the partition key does not have.

**Fix direction:** implement `RkyvSupport` for the key types a sort field may be (the integers,
and tuples of supported types), and check that the archived ordering a sorted partition relies on
is the tuple's own `Ord`. Then item 42 is the SHQL half of it.

### 35. A `RefCell` borrow is held across three awaits in the compactor

```rust
if let Some(entry) = self.map.to_archive.borrow().get(partition) {
    let handle = self.map.get_archive(&entry.archive).await?;
    let read = handle.read_at(entry.offset, entry.size).await?;
    let archived = <T as RkyvSupport>::access(&read)?;
    handle.close().await?;
```

`.../fs/compactor.rs:232-241`

`shoal-core` is edition 2021, where the temporary `Ref` produced by `.borrow()` lives to the end
of the `if let` statement. So the shared borrow on `to_archive` is held across all three awaits,
during which other tasks on the same executor run.

It does not panic today, because the only `borrow_mut` callers — `set_partition` and
`remove_partition` (`.../fs/map.rs:442-459`) — are the compactor itself, which is not running
while it is parked here. It is listed because that is a property of who happens to call what, not
of anything enforced, and because the fix is already sitting next to it:
`ArchiveMap::find_partition` (`.../fs/map.rs:514-520`) exists to copy the entry out, `ArchiveEntry`
is `Copy`, and the loader path already uses it.

Worth reading against the edition too: this becomes correct for free under edition 2024's
temporary scoping, which means an edition bump would silently change the failure mode rather than
the code. **The workspace is already mixed** — `shoal-bench` and `shoalctl` are edition 2024 while
`shoal`, `shoal-core` and `shoal-derive` are 2021 — so the bump is a per-crate decision that has
already been made three times without this being considered.

### 47. A torn tail on the active log is counted as data loss

`FileSystem::read_intent_log` (`.../storage/fs.rs:202-206`) counts a `truncated` reader
against `RecoveryStats::truncated_logs`, and it does so for the active log on the same terms as
for an inactive one:

```rust
// a reader that stopped on a bad tail dropped everything after it
if reader.truncated {
    stats.truncated_logs += 1;
}
```

`RecoveryStats::is_clean` treats any non-zero `truncated_logs` as loss
([Recovery](../storage/recovery.md#what-recovery-discards)), so `Shard::report_recovery` emits
`WARN Recovery discarded data` — after an ordinary crash, where nothing was lost.

A torn tail on the active log is what a crash *looks like*. Writes are acknowledged only after
they are durable — that is [items 1-3](resolved/durability.md), and `ack_survives_sigkill`
(`shoal/tests/persistent_sorted_table.rs`) is the end-to-end proof — so the half-written entry at
the end of the log belongs to a write no client was ever told about. Dropping it is the design
working.

This matters by the recovery page's own argument. `updates_after_delete` is kept out of
`is_clean` because "an update that lands on a row a delete already tombstoned is correctly
dropped, and counting it as loss would make the numbers that do mean loss useless"
([storage.rs](../storage/recovery.md#what-recovery-discards)). Counting benign torn tails is the
same mistake in the opposite direction: every unclean shutdown produces a `WARN` that claims
data was discarded, so the warning that means real corruption is buried in warnings that mean a
process was killed. [Item 44](resolved/compaction-tail-loss.md) draws this same distinction
correctly on the compaction side.

**Fix direction:** the reader already knows where it stopped. A tail whose remainder is padding
or zeros is the benign shape; damage with non-zero bytes after it is a corrupt record with data
behind it. `IntentLogReader` can scan the remainder once on the way out and set two different
flags. Failing that, the cheaper split is positional — count damage in the *active* log
separately from damage in an inactive one, since an inactive log has been fully written and
rotated, so damage in it is never benign. Either way `is_clean` should ignore the benign counter,
the way it already ignores `updates_after_delete`.

### 235. The client acceptor drops the TLS session its comment says it keeps for the connection's life

`shoal-core/src/server/shard.rs`, `client_acceptor`; `shoal-core/src/server/peer/listener.rs`,
`peer_acceptor`

**Low: nothing reads the session today.** After the TLS handshake the keys are in the kernel, and
rustls' `Established` keeps the `KernelConnection` that would handle a key update or a
`NewSessionTicket` the peer sends. The client acceptor binds it as `_tls` with the comment "the
established session is bound rather than dropped so that rustls' record of it outlives the socket,
which is where a key update would be handled". But the binding is inside the `async` block run
under the handshake's deadline, so the session is dropped when the handshake ends, not when the
connection does. The peer listener keeps only `established.peer` and drops the rest at once.
Shoal sends no key update ([F14](../features/encryption-in-transit.md#limitations)), and a
connection whose peer sent one would fail its next read, with or without the record.

**Established by reading the source**, while building [F84](../features/replication-lane-handoff.md),
which hands a replication lane between executors and would have had to carry the session with it
had either acceptor kept one. X11's spike carried it, since its server held it for the
connection's life.

**Fix direction:** decide whether a key update is ever to be handled. If so, hold the session for
the connection's life in both acceptors, and carry it in `HandedLane` beside the descriptor. If
not, drop the binding and correct the comment, so nothing claims a record that is not kept.

### 248. `shoaladm bench` left an empty device directory it had made

`shoaladm/src/bench/hosts.rs`, `make_devices_script` and `remove_made_dir_script`; the run's
teardown in `orchestrate.rs`

**Low: an empty directory where a device was.** After [F89](../features/scrub-and-repair.md#on-the-lab)'s
rate leg, two runs of `get100` at 256 MiB with a deep scrub on a copy of the lab's three nodes at
`c700546`, titan held `/xfs/shoal-tmdb-pool-bench`, an empty directory owned by the node's user and
last changed sixteen seconds before the leg ended. The bench had made it (`sudo -n mkdir`) and
records a directory it made, to `rmdir` once empty on every way out; europa's and hyperion's, and
titan's `/xfs2` one, were gone. An empty directory is claimed as a new device, so a later run is not
refused by it, but it is a host change the bench did not undo, and a run that finds it there no
longer counts it as made and leaves it for good.

**Established by observing it once**, on the host, after the run; it was removed by hand. Not
reproduced, and the cause is not established. One candidate: the made directory's `rmdir` ran while
the node still held files in it, before the cluster's teardown emptied it, and its failure is
silenced (`2>/dev/null || true`).

**Fix direction:** undo a made device directory only after the run's cluster is torn down, and say
so when the `rmdir` fails rather than silencing it.

---

## Unsafe `Send` invariant

Not a bug, but the most dangerous thing in the codebase to change without knowing. **It was a
bug once:** [Resolved #133](resolved/read-plan-rc-across-shards.md) was an `Rc` inside
`QueryMetadata`, which crosses shards on every split get and which this section did not name.
Anything a query carries between shards must be `Send` on its own, and
`what_a_query_carries_between_shards_is_send` checks `ReadPlan` and `QueryMetadata`, and since
[F84](../features/replication-lane-handoff.md) `HandedLane`, the replication lane one executor
hands another.

```rust
/// # Safety
///
/// The Partition variant should not be sent across threads ever.
unsafe impl<D: ShoalDatabase> Send for ServerMsg<D> where ... {}
```

`shoal-core/src/server/messages.rs:248-257`, and similarly for `Comms`
(`shoal-core/src/server/comms.rs:16-23`).

`ServerMsg::Partition` carries a glommio `ReadResult`, which is not `Send`. The whole enum
asserts `Send` anyway so it can travel kanal channels. The invariant — a `Partition` message
may only ever be sent on its own shard's channel — is upheld solely by `FsLoader` being
constructed with a clone of its own shard's sender and no other
(`.../fs/loader.rs:256-271`).

Give a loader, compactor, or any task a sender belonging to a different shard, and `Partition`
messages become cross-thread: undefined behaviour, no compiler error, and probably no test
failure. Any change touching loader construction should be read against this.

**There is now a second place that has to hold, and it is not the constructor.** `FsLoader` keeps
a `senders: Vec<AsyncSender<ServerMsg<D>>>` reuse pool: `spawn_task` pops a sender for the read
task to report on and falls back to cloning `self.shard_local_tx` (`.../fs/loader.rs:292-295`),
and a finished task hands its sender back into the pool (`:370-372`, `:386`). So the invariant is
no longer "the loader holds one sender it cloned at construction" but "**every** sender that ever
enters that pool is a clone of this shard's". It holds today because the only two things that put
one there are the fallback clone and a task that was handed one from the same pool. A future
change that seeds the pool from anywhere else — a shared pool across loaders, most obviously —
breaks the invariant without touching the constructor this section used to point at.

The related `messages.rs:223` panic is the same invariant seen from the other side: cloning a
`Gathered` message is refused because a broadcast would be the way one reached a shard it was not
addressed to.

---

## Suggested triage order

1. **Item 38** — not a production defect, but the test suite is what every other fix on this page
   is judged by, and right now **five** of its binaries can silently serve each other's traffic.
   Worth doing before the fixes below rather than after them. It said "two" until the
   [August 2026 review](review-2026-08.md) re-measured, which is the argument for doing it rather
   than tracking it: every new test binary that starts a server joins the collision by default.
2. ~~**Items 124 and 122**~~ — ~~item 16, the hot-path panics~~ [resolved](resolved/hot-path-panics.md),
   and these are what fixing it found. Both are resolved too:
   [124](resolved/unsorted-update-generation.md) and [122](resolved/intent-log-failure.md). 124 was a stale read after an eviction nobody would think
   to look for; 122 was the storage failure item 16 was always said to be about, which ended the
   shard rather than panicking it. This entry used to read "items 11 and 16"; item 11, the
   empty-ring window a client could hit during startup, is [resolved](resolved/tablet-ring.md) and
   was made unbuildable rather than checked.
3. **Item 22** — the size accounting the whole memory limit rests on, now with *four* different
   bases for the same field. It moved up this list because the two things that used to sit in
   front of it are done: [item 6](resolved/memory-accounting.md) fixed the path that destroyed the
   counter outright, and [item 13](resolved/eviction-log-underflow.md) turned the eviction log into
   something that reports drift instead of breaking on it. That `drift` field is the instrument for
   this one, and it now has a first hypothesis to test rather than a whole page to reason about:
   recovery adds a partition in archive bytes and eviction takes it off in deep size, which
   predicts drift proportional to how many partitions a shard's recovery loaded and none on a
   shard that started clean.
4. **Item 47** — cheap, and it is making the recovery counters harder to trust the longer it sits.
   Every unclean shutdown currently reports discarded data, so the warning that means real
   corruption is buried under warnings that mean a process was killed.
5. ~~**Items 27 and 42**~~ **Item 42** — data that SHQL cannot reach at all: ~~a partition key
   containing a quote, and~~ a composite sort key. Item 27, the quote, is
   [resolved](resolved/shql-quote-escape.md): a literal doubles it, as SQL does. Item 42 was
   already the sharper of the two now that [item 8](resolved/sort-keys.md) is fixed, since a
   sort key is a thing you can query with.
6. ~~**Items 32 and 33**~~ — two leaks with one shape: state keyed by something that goes away and is
   never told. Both are resolved: [33](resolved/gather-expiry.md) at M5, and
   [32](resolved/client-gone-broadcast.md) by sending the `ClientGone` broadcast the peer lanes
   already sent ([Resolved #94](resolved/disconnected-client-cleanup.md)) for ordinary clients
   too. The cost it was waiting on is one message per shard per disconnect, the same as
   `NewClient` per connect; the capture of a connection-churn workload is still owed.
7. ~~**Items 43 and 46** — the two remaining holes in the storage marker. Worth doing together,
   since both are changes to what `StorageMeta::claim` looks at before it writes.~~ Both are
   resolved: [43](resolved/marker-every-root.md), and
   [46](resolved/unmarked-directory-refused.md) by a claim that judges every root before it
   writes to any.

### 52. A resident hit in `exists` answers a query a blocked clone will answer again

`PersistentSortedTable::exists` (`.../persistent/sorted.rs:684-760`) walks the partition keys an
exists named. A key whose partition has to be read from disk pushes a clone of the whole query
into `self.blocked` — through `block_on_load`, which is where the clone is parked — and moves on
(`:711-725`):

```rust
if check_disk {
    // build a query for just this blocked partition
    let blocked_exists = SortedQuery::Exists(exists_query.to_blocked(*partition_key));
    // park this exists if this partition has to be read from disk first
    if self.block_on_load(*partition_key, &meta, blocked_exists).await {
        // remember that we are still waiting on this partition
        blocked.push(*partition_key);
        continue;
    }
}
```

A *later* key in the same loop that is resident and does hold a matching row returns straight
away (`:733-741`):

```rust
if partition.exists(exists_query, &mut seek) {
    return Some((meta.client, meta.id, meta.stamps, response));
}
```

That early return never reaches the `self.pending_exists.insert` at the bottom of the loop
(`:749`), and it does not remove the clone from `self.blocked`. When the partition finishes
loading the clone is replayed and answers the same `(id, index)` a second time. The client is owed
exactly one response per query, and an unordered stream will surface both.

The refactor that introduced `block_on_load` moved the parking out of this function and did not
change the shape of the defect: the clone is parked by the callee and the early return is still
the caller's.

Only reachable when one exists names partitions on the same shard where at least one is
evicted and a *later* one is resident and matching — the order matters, since a resident hit
before the blocked key would have returned before the clone was ever parked.
`PersistentUnsortedTable::exists` has no such path: it names one partition and has one exit.

**Established by reading the source**, while building [F6](../features/stage-breakdown.md).
The stage report counts these as `join.duplicates` rather than folding them into a bucket, so
a run that hits this says so — but nothing yet reproduces it.


### 53. `hotpath`'s `percent_total` is meaningless for a concurrent scope

`docs/perf/runs/*.hotpath.json`, every capture

`hotpath` reports a `percent_total` per scope, and it is not a percentage of anything a reader
would take it for. It is not normalised across scopes that ran at the same time on different
shards, so twelve shards each spending most of a run inside a scope sum to far more than the run
did. The committed `B1-performance.hotpath.json` reports:

```
shoal_core::server::tables::storage::fs::stream::write_helper   percent_total: 1253041
shoal_core::server::shard::handle_query                         percent_total: 6245
```

Those are 12,530% and 62%, of a run that was 100% of itself.

The field is parsed and kept (`shoal-bench/src/model/hotpath.rs:35`) and never plotted or
tabulated, which is the right treatment. The trap is that it looks exactly like the
number anybody would reach for first. `total` — nanoseconds summed across every shard that entered
the scope — is the field to rank by, and it needs saying that it is a sum across shards rather
than a share of the wall clock, which is why the chart's axis on
[Benchmark Results](../performance/overview.md) says so.

**Established by reading the committed artifacts**, while building
[F7](../features/bench-runner.md). `shoal-bench` never plots or tabulates the field, and
`render::chart::hotpath_scopes::tests::the_unnormalised_percentage_is_never_drawn`
(`shoal-bench/src/render/chart/hotpath_scopes.rs:149-186`) pins that by feeding a scope a
`percent_total` of 12,530 and asserting the number never reaches the chart.
Fixing it properly is upstream in `hotpath`, or means dividing by the shard count that actually
touched each scope — which the profile does not record.

### 54. A schema still declares `rkyv` and `deepsize2` itself

`shoal-proto/src/shared/traits/sorted.rs:10`, `unsorted.rs:10` and `traits.rs`, the `DeepSizeOf`
supertrait bound; and every schema's own `#[derive(Archive, Serialize, Deserialize)]`

**Mostly fixed — see [Resolved #54](resolved/macro-emits-three-crates.md) for the part that is
done and why.** The macros no longer name any crate by path: they emit `::shoal::` and the facade
re-exports what they reach for, and [F15](../features/client-server-split.md) took glommio out of
a client's dependency graph rather than re-exporting it. `glommio`, `uuid` and `kanal` are gone
from the manifests that carried them under a comment saying they should not have been there.

What remains is not a macro path and could not be fixed by re-pointing one. A schema writes

```rust
use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
```

by hand, and both derives expand to absolute paths into their own crates, so both have to be
declared. `deepsize2` is there because `ShoalTableSupport` is bound on `DeepSizeOf` — the server
sizes a row to charge it against the memory budget — and that bound is visible to a client that
will never evict anything.

The original filing said the generated code "names `glommio`, `uuid` and `deepsize2` by path".
That was wrong about `deepsize2`, which no macro has ever emitted; the third name should have been
`kanal`. Corrected on the resolved page rather than here.

**Established by reproducing it**, while building [F8](../features/purpose-built-workloads.md).

**Fix direction:** two options, both filed in [TODOs](todos.md). Emit the `DeepSizeOf` impl from
`#[derive(ShoalSortedTable)]` by summing the row's fields, which removes the derive from the
schema author's hands but hand-rolls an accounting impl the eviction budget depends on being
right; or take the bound off the client-visible traits and put it where the server needs it.
`rkyv` is harder and may not be worth closing: a schema's types are rkyv types, and saying so is
arguably honest rather than leaky. `shoal::rkyv` and `shoal::deepsize2` exist as escape hatches
meanwhile.

### 55. A get that found nothing is reported as a query that failed

`shoal-core/src/client.rs:313`, `Shoal::send_one`

`send_one` calls `suceeded` (`:816`) on the response and turns a get that matched no rows into
`Err(QueryDidNotSucceed)`. "The row is not there" and "the query did not work" are different
answers, and a caller that wants the first has no way to ask for it through `send_one` — it has to
drop to `send` and drain the stream itself, or use `exists`, which only answers a yes-or-no.

This is a usability defect rather than a correctness one, and it bites in a specific way: any code
that probes with a get treats an empty table as a broken server. The F8 readiness probe did exactly
that and timed out for thirty seconds against a server that was answering every query correctly,
until it was changed to use `exists`.

**Established by reproducing it**, while building F8. `QuerySuceededOpts` already exists as the
knob that decides what counts as success, so the fix is plausibly to let `send_one` take one rather
than always using the default.

**Unblocked, not closed, by [F11](../features/error-channel.md).** "Found nothing" and "failed" are
now different answers on the wire, and `suceeded` reports the second as `Errors::Server` whatever
the options say. What is left is the first half: letting `send_one` say that an empty get is
acceptable. It no longer has to, to tell the two apart — a caller can ask `response.error()`
directly — but it is still the ergonomic gap this item was filed for.

### 59. A shard that cannot free anything keeps trying, on every message, in silence

```rust
// check if we need to evict any data
if *self.memory_usage.borrow() > self.conf.resources.memory {
    // try to evict our least recently used data
    self.evict_data().await?;
}
```

`shard.rs:984-988`, and `Shard::evict_data` at `:847-882`

The trigger is a level, not an edge: it is re-tested at the bottom of **every** iteration of the
shard loop. `evict_data` answers it by draining the LRU until it has found 40% of current usage
worth of victims, or until the LRU runs out:

```rust
// we have no more rows we could evict even if we wanted too
None => break,
```

`shard.rs:872-873`

When the LRU is empty that arm is taken immediately, the function allocates a
`HashMap::with_capacity(10)` (`:852`), evicts nothing, and returns `Ok(())`. The condition above
it is still true, so it runs again on the next message, and on every message after that, for as
long as the process is over its limit. Nothing is logged: the `None` arm is the only exit and it
says nothing, and the eviction event ([Resolved #13](resolved/eviction-log-underflow.md)) is
emitted per *pass that dropped something*.

So a shard that is over its memory limit and has nothing it is allowed to drop looks, from
outside, exactly like a shard that is under it.

**This is reachable rather than theoretical, and [F9](../features/ephemeral-tables.md) is what
makes it so.** An ephemeral partition never enters the LRU, because `NoStorage` never sends a
`MarkEvictable` — which is the invariant ephemeral data being safe rests on, and is correct. But
ephemeral rows *are* counted in `memory_usage`. A database whose ephemeral tables alone exceed
`resources.memory` therefore sits permanently in this state. The ephemeral page says memory is not
bounded and that bounding it is the caller's problem
([Limitations](../features/ephemeral-tables.md#limitations)); what it does not say is that
exceeding it is silent and costs a pass per message.

The cost of the pass itself is small — a `RefCell` borrow, a comparison, one allocation and an
empty pop. It is filed here rather than in [Optimizations](optimizations.md) because the defect is
the silence, not the work.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md).
Nothing reproduces it, and the cheapest reproduction is an ephemeral table and a small
`resources.memory` — which `ephemeral_sorted_table.rs`'s "memory pressure evicts none of it" test
already sets up, and asserts the data survives rather than what the shard did to keep it.

**Fix direction:** distinguish "nothing needed freeing" from "nothing could be freed". The `None`
arm knows which it is — `need` is still non-zero — so it can emit one `WARN` naming the shortfall.
Rate-limiting it is the whole difficulty: at one per message it is worse than silence. The honest
shape is probably a latch, warning on entry to the state and again on leaving it. Note this should
*not* be fixed by making the trigger an edge — a level trigger is what lets a shard recover when a
partition becomes evictable later.

### 60. A result stream that is not drained to the end leaks its slot in the client

**The client half is [resolved](resolved/stream-connection-accounting.md)**: both stream types
remove their slot on `Drop`, and a frame for a slot whose reader is gone is dropped and the slot
removed rather than ending the connection's read loop (item 130). ~~What is left open is the server
half below - nothing tells the server to stop producing answers for a stream the client dropped -
and the channel pair a dropped stream does not return to the reuse queue.~~ **The server half was
delivered by [F75](../features/client-cancel.md)**: a stream dropped with answers owed cancels them,
and the server stops writing them and runs none of its reads it has not started. What is left open is the
channel pair a dropped stream does not return to the reuse queue, a missed reuse and not a leak.
The rest of this entry is the state before those fixes.

`ShoalResultStream::next` releases the client-side state for a query only when the response it
just returned was the last one:

```rust
if end {
    // remove this stream id from our channel map
    self.channel_map.pin().remove(&self.id);
    // take the ends of our channel
    if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
        self.channel_queue_tx.send((tx, rx)).await?;
    }
}
```

`shoal-core/src/client.rs:1032-1039`, and the same block in `ShoalUnorderedResultStream::next`
at `:1231-1238`

Neither stream type implements `Drop`. `Shoal` does (`:467`) and it does not walk `channel_map`.
So there are three ways to end a stream without reaching that block:

| How | What is left behind |
| --- | --- |
| Drop the stream before its last response | One `channel_map` entry, and a channel pair that never returns to `channel_queue` |
| ~~`next()` returns `Err` — a failed `access`, a closed proxy~~ | ~~The same~~ — fixed by [F11](../features/error-channel.md): both `next()`s now run the release block on the `Err` path as well as the `end` path |
| `skip()` past the end, or any early `return` in the caller | The same |

The `channel_map` entry is what the proxy looks a response up in (`:536`), so the leak is not only
memory: every response the server later sends for that query id is delivered into a channel with
no reader, which is unbounded. And `channel_queue` is a reuse pool, so a leaked pair is a channel
the next query has to allocate instead of reusing (`:188-191`).

**This is the client-side twin of [item 32](resolved/client-gone-broadcast.md)**, since resolved,
— state keyed by something that goes away, with nothing told about it — and it has the same
consequence, which is that an ordinary well-behaved caller leaks. It is bounded differently,
though: the server's leak is per connection and this one is per query.

`send_one` and `exists` are safe today, and it is worth saying why, because it is not obvious. Each
sends a single-query bundle, so the one response it reads back *is* the end of the stream and the
block above runs. Change either of them to read one response out of a multi-query bundle and they
stop being safe.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md). It has
never been observed, and it could not have been: the streaming APIs have
[no test at all](test-coverage.md#the-streaming-client-apis), and every integration test goes
through `send_one`, `exists`, or a fully drained `send`.

**Fix direction:** a `Drop` impl on both stream types doing what the `end` arm does. The obstacle
is that returning the channel pair to `channel_queue` is an `async` send, which `Drop` cannot
await — so either the queue gains a synchronous `try_send` for this path, or `Drop` removes the
map entry only and lets the pair be dropped. Removing the map entry is the half that matters;
losing a pooled channel is a missed reuse, not a leak.

**`Drop` alone fixes the client and leaves the server wrong**, which is worth knowing before taking
the easy half ([D6](../direction/connection-pool.md#drop-on-both-stream-types)). Once the entry is
gone, the responses the server is still producing arrive at a proxy that cannot find a channel for
them, ~~which today returns `Errors::ProtocolError` and kills the read task for that connection~~ —
[F11](../features/error-channel.md) made that a `WARN` and a `continue`, precisely because killing a
connection over one caller's leak takes every other query on it down too. So the easy half is no
longer actively harmful, and it is still half. The pair that is actually correct is `Drop` plus a `Cancel` message telling
the server to stop, and `Cancel` is a message type the wire format ~~**has a discriminant for and no
wiring behind** since [F10](../features/framing-and-protocol-evolution.md) — so this is no longer
blocked on a flag day, only on the send and the handler~~ wires since
[F75](../features/client-cancel.md), which is that pair
([D2](../direction/framing.md#message-types)).

### 62. A server that has exited leaves its client connections open

`ShoalPool::exit` (`shoal-core/src/server.rs:104-117`) sets `should_shutdown` and joins the shard
threads. The listener goes with them — a connection attempt to the port is refused afterwards — but
the sockets of connections that were already **established** are not closed. A client holding one
sees no `FIN`, so its read loop stays parked in `read_exact` forever and any query written to that
connection waits for a response no thread is left to produce.

The per-connection tasks `client_acceptor` spawns hold the `TcpStream`s, and nothing drains or
cancels them before the executor goes away. When the server runs in the same process as the client —
which every integration test does — there is no process exit to have the kernel clean up after it
either.

**Established by reproducing it**, while building [F11](../features/error-channel.md). A test that
started a server, called `pool.exit()`, and then sent one query never got an answer; instrumenting
`TcpProxy::start` showed the read loop had not returned for any of the pool's ten connections, so
the client-side sweep F11 added had nothing to fire on. Connecting to the port after `exit()`
returns `ECONNREFUSED`, which is what separates "the listener closed" from "the connections closed".

This is the server-side twin of the gap F11 closed on the client. F11 made a connection that
*ends* tell the queries it owed; this is a connection that never ends.

**Fix direction:** two halves, and the first is small. `client_acceptor`'s per-connection tasks
should be held rather than detached, and cancelled on shutdown, so the sockets are dropped before
the executor is — which turns this into an ordinary EOF that the client already handles.
The second half is that a client cannot rely on the peer being well behaved about it, so it wants a
deadline: a `Ping`/`Pong` health check ([item 23](#23-client-stream-and-pool-rough-edges),
`MessageType::Ping` is reserved and unwired) or a per-query timeout
([TODOs](todos.md)). Neither is the error channel — F11 gave a failure somewhere to go and cannot
invent a failure nobody detected.

### 63. Nothing limits how often a peer may guess a password

`server_auth` (`shoal-core/src/server/shard.rs`) runs an exchange for anything that reaches the
port and completes a handshake, and there is no counter anywhere — not per source address, not per
username, not per shard. A peer can open a connection, guess, be refused, and open another one, as
fast as it can complete three round trips.

The asymmetry is what makes it worth filing rather than shrugging at. Each guess costs the
**server** one PBKDF2-HMAC-SHA-256 at the credential's iteration count — on the order of a
millisecond of CPU on a shard that is single-threaded and is also serving queries — while costing
the client a `format!`, because a client that intends to fail does not have to derive anything. A
peer that sends a syntactically valid proof of the wrong 32 bytes gets the server to do all of the
work and does none of it.

That makes this two defects wearing one number. It is a **credential** exposure, and it is more
immediately a **denial of service** one: a few hundred connections a second is a shard spending
its time on PBKDF2 rather than on the table it owns, without ever authenticating.

**Established by reading the source**, while building
[F12](../features/authentication.md). Not reproduced, and the reproduction is the interesting part:
what a working `connect` workload ([O30](optimizations.md)) would have to show is how many refused
exchanges per second it takes for a resident get's p99 to move.

**Fix direction:** the cheap half is a per-connection cap of one exchange — a peer that fails is
already closed, so what this really bounds is reconnection, which belongs with a per-source-address
rate limit and does not exist. The half that removes the asymmetry is to do the derivation
**after** cheap validation and to bound concurrent in-flight exchanges per shard, so that a flood
queues rather than compounds. Note the decoy path ([F12](../features/authentication.md)) constrains
the shape of any fix: whatever is added must cost the same for a user that exists and one that does
not, or it becomes the enumeration oracle the decoy exists to prevent.

### 64. Fifteen pages cite the client at a path it left when the crates were split

**Filed as four `direction/` pages and it is fifteen pages across four chapters** — see the widened
scope at the end of this item.

Every page in the direction chapter that argues from the zero-copy response path quotes the same
three lines and cites them as `shoal-core/src/client.rs:524-527`, `TcpProxy::start`. The read is now
at `client.rs:1063-1066` and lives in `TcpProxy::read_frame`, because
[F10](../features/framing-and-protocol-evolution.md) split frame decoding out of the relay loop. The
quoted code is also one refactor stale — it reads `len` where the tree reads `frame.rest_len`.

| Page | What it cites |
| --- | --- |
| `direction/transport.md` | `client.rs:524-527` — inside D1's argument that QUIC's crypto ends the zero-copy read |
| `direction/overview.md` | `client.rs:524-527` — inside "the constraint every page inherits", which every other page refers back to |
| `direction/authentication.md` | `conf.rs:132-138` for `Networking`, which is now `conf.rs:138-157` and has a third field |
| `api/client.md` | the same read, in the `ShoalResponse` section |

`direction/encryption.md` carried the same citation and is corrected, which is how this was found.
The other four are left as they are rather than fixed in passing, because a sweep that fixes one
stale citation and not the fifteen others on the same pages is the drift this item is about.

**Established by reading the source**, while correcting [D4](../direction/encryption.md).

**Fix direction:** the same treatment this page already gives itself — a
re-resolution pass over every `file:line` in `docs/src/direction/` and `docs/src/api/`, with the
symbol name kept beside each one so the next drift is greppable. ~~The August 2026 review did this
for the appendix and did not cover the direction chapter, which had just been written and was
correct at the time.~~

**Both halves of that sentence are now wrong, and the item is larger than it was filed as.**

*The appendix is no longer clean.* [F15](../features/client-server-split.md) moved the client into
its own crate after the review, so the citations did not drift by lines — the file they name stopped
existing. `shoal-core/src/client.rs` is cited **26 times across 15 pages**, and two of them are on
[Optimizations](optimizations.md), the page whose whole function is to say where a cost lives.
Those two, and six more that drifted the ordinary way, are corrected; the remaining pages are not.

*And the line this item quotes is itself stale twice over.* It says the read "is now at
`client.rs:1063-1066`". That was true of `shoal-core/src/client.rs` when this was written and there
is no such file; the read is at `shoal-client/src/client.rs:1489-1492`, and it has since acquired a
third defect worth naming —
~~[O37](optimizations.md#o37-the-client-zeroes-a-response-buffer-and-immediately-overwrites-it), the
`resize(len, 0)` that memsets a buffer the next line overwrites~~ — since
[done](../features/read-buffers-are-filled-not-zeroed.md), so that quoted read is now stale a third
time and in a third way: the lines it names hold a different call. **An item about stale citations
went stale**, twice, which is the strongest possible argument for the fix direction it proposes.

The scope is therefore `docs/src/direction/`, `docs/src/api/`, `docs/src/architecture/` and the
remainder of the appendix — and the pass is worth doing as one sweep with the symbol names added,
rather than as fifteen incidental corrections, for the reason this item already gives.

### 66. The release profile nothing has been reading

`shoalctl/Cargo.toml:28-30`, and the absence of a `[profile.release]` in `Cargo.toml`

`shoalctl` sets `codegen-units = 1` and `lto = true`. Cargo reads `[profile.*]` only from the
workspace root, so both lines have been ignored for as long as they have existed — cargo says so
on every build:

```
warning: profiles for the non root package will be ignored, specify profiles at the workspace root:
package:   /home/mcarson/projects/shoal/shoalctl/Cargo.toml
workspace: /home/mcarson/projects/shoal/Cargo.toml
```

There is no `[profile.release]` at the root either, so **every release build in this workspace —
the server, `shoal-workload`, and every benchmark capture ever taken — uses the default profile:
`lto = false`, `codegen-units = 16`.** This is the same class of trap the root manifest already
records about a `[build]` table being silently ignored there (`Cargo.toml:23-25`).

It is not only a missed optimization. Without LTO, rustc will not inline across a crate boundary
unless a function is generic, `const`, or carries `#[inline]` — which is why splitting the crate
in [F15](../features/client-server-split.md) had to add thirty `#[inline]` attributes, and why
doing so turned out to *improve* the header codec by up to 80%: those calls had always been
cross-crate from the benches and the client, and had never been inlinable.

**Established by reproducing it** — cargo emits the warning above on every command run in this
workspace.

**Fix direction:** decide what the release profile should be and put it at the root. Note that
adding one changes the codegen of every binary and therefore invalidates every stored capture, so
it needs its own before-and-after and a note on
[Performance Baseline](../performance/baseline.md). Whether `lto = "thin"` would let
the thirty `#[inline]` attributes be dropped again is the interesting half of the question.

### 69. `shoalctl` and the tests still install no tracing subscriber

**Mostly resolved.** [F34](../features/benchmark-tracing.md) put a subscriber on `shoal-workload`,
so a benchmark capture honors the `tracing:` section of its config and the engine's spans can be
seen — [the resolved page](resolved/benchmark-tracing.md) has the whole of it. What is written
there and not here is closed. Three things are not.

`shoalctl/src/main.rs`, and every test in the workspace

**`shoalctl` installs nothing**, so an operator using it to talk to a server gets no structured
output from the client half — including the `event!(Level::ERROR, ...)` calls that report a dead
connection or a refused frame. The fix is the one `shoal-workload` took: call `trace::setup_with`
in `main` and hold the guard. It has not been done because nothing has needed it yet, which is a
reason to file it rather than a reason it is fine.

**No test can observe any event the server emits**, and this one cannot be fixed the same way.
`setup` installs a **global** subscriber, so the first test to call it decides what every other
test in that binary sees — which is why `shoal/tests/disk_lookups.rs` installs its own local one
rather than reaching for `setup`. The recovery summary has no automated coverage for exactly this
reason ([Test Coverage](test-coverage.md)). Closing it needs a **non-global** path out of
`trace.rs` — something returning a `Subscriber` for a caller to scope with
`tracing::subscriber::with_default` — and F34 did not build one.

**Whether a library should install a global subscriber at all is still unmade.** F34 sidestepped it
by putting the install on the binary, which is right for the benchmark and says nothing about a
program embedding `ShoalPool::start` and expecting the config it handed over to be honored. That
program today gets silence unless it calls `setup` itself, and nothing tells it so.

**Fix direction:** the `shoalctl` half is three lines and is unblocked. The test half wants
`trace::subscriber(conf, &TraceOptions) -> impl Subscriber` alongside `setup_with`, with `setup_with`
built on it — then a test scopes one for its own duration and the global install stays the binary's
decision.

### 70. An axis writes more digits than its ticks have room for, and twice labels two at the same number

`shoal-bench/src/render/chart/sweep.rs`, the `y_label_formatter` passed to `themed_mesh!`

`chart-grid-latency` draws `900.00 µs`, `1.00 ms` and `1.00 ms` as three consecutive y ticks, and
`chart-row-size-latency` does the same at several places on its axis. Two gridlines carrying the
same label is a reader counting decades wrong: the chart says the two lines are at the same value
and they are not.

The cause is rounding, not placement. plotters chooses key points on a logarithmic axis at ratios
that are not round numbers, and `fmt::duration_ns` writes two decimal places in each band, so
1,000,000 ns and 1,004,000 ns both come out `1.00 ms`. Nothing detects the collision because the
formatter sees one value at a time and has no memory of the previous tick.

[F19](../features/chart-legends.md) fixed this for the **x** axis of every sweep, by ticking at the
values the workloads were measured at instead of at a spacing plotters chose — there are never many
of those and they are exact. The y axis has no such list: it is a continuum of measured costs, and
picking ticks for it means choosing them rather than reusing them.

**The same formatter crowds an axis it does not duplicate.** `chart-hotpath-scopes` ticks at
`0.00 ns`, `50.000 s`, `100.000 s` … `450.000 s`, spaced 38 user units apart. `150.000 s` is nine
characters at a 9.68 unit font, so two adjacent labels want about 44 units between their centres
and have 38. Nothing here is wrong, only unreadable, and the cause is the same one: a formatter
that writes a fixed number of digits per band, chosen without reference to how much room the axis
gave it. Three decimals in the seconds band exist so that a 1.802 s measurement in a *table* keeps
its precision; on an axis whose ticks are round multiples of fifty they are three characters of
nothing. This half is older than item 70's other half and predates
[F19](../features/chart-legends.md) — `hotpath_scopes.rs` is untouched by it.

**Established by reading the generated pages.** `sed -n '/chart-grid-latency/,/<\/svg>/p'
docs/src/performance/grid.md | grep -E '^[0-9.]+ (ns|µs|ms|s)$' | uniq -d` prints `1.00 ms`. The
crowding is read off the tick anchors in `docs/src/performance/attribution.md`, which are `x="..."`
attributes 38 apart on the `y="348"` row.

**Fix direction:** give the value axis a key-point list of its own — round numbers of the unit the
range lands in, one per half decade — rather than deduplicating labels after the fact. Dropping the
duplicate leaves an unlabelled bold gridline, which is a smaller lie than the current one but is
still a chart that has a gridline it will not name. `sweep::Scale` already takes a tick list, so
the mechanism exists; what it needs is a generator for it.

Everything that has been fixed, and why it was fixed the way it was, is in
[Resolved Issues](resolved-issues.md). The SHQL parser has gained test coverage at both stages
([SHQL](../api/shql.md#testing)) — items 26–29 were found while writing it.

### 71. `throughput_sensitive` is configured, documented and mostly unused

`shoal-core/src/server/tables/storage/fs/map.rs:415` and `:433`, `ArchiveMap::get_active_writer`

`FileSystemThroughputWriterConf` carries a `buffer_size` and a `write_behind`, both defaulted, both
documented in [Configuration](../getting-started/configuration.md) as the settings for "the lower
latency but high throughput sensitive io". Exactly one writer reads them:

```rust
// map.rs:403 - ArchiveMap::new_writer, the map's own intent log
let writer = DmaStreamWriterBuilder::new(file)
    .with_buffer_size(self.conf.throughput_sensitive.buffer_size)
    .with_write_behind(self.conf.throughput_sensitive.write_behind)
    .build();
```

The writers for the archives themselves do not:

```rust
// map.rs:415 - the cached-handle branch
return Ok(DmaStreamWriterBuilder::new(file.dup()?).build());
// map.rs:433 - the freshly-opened branch
Ok(DmaStreamWriterBuilder::new(file).build())
```

Both build with glommio's defaults. `map.rs:224`, the temporary file a map snapshot is staged
through, does the same. So the section named for throughput governs the intent log of the archive
*map* — a small, latency-shaped write — and not the bulk data path it appears to name. A deployment
that raises `throughput_sensitive.buffer_size` to tune bulk ingest changes nothing about how the
archives are written.

There is a second, smaller wart in the same struct. `FileSystemThroughputWriterConf::write_behind`
is a **count** and carries `deserialize_with = "utils::deserialize_byte_size"`
(`fs/conf.rs:154-156`), so `write_behind: "4Ki"` parses to 4096 in-flight writes. The matching field
on the latency writer has no such annotation. This is already noted in
[Configuration](../getting-started/configuration.md) and is filed here so it is fixed alongside the
line above rather than separately.

**Established by reading the source, and since reproduced by measurement.**
[F20](../features/configuration-sweeps.md) swept both settings for exactly this reason —
`macro/conf/storage/throughput_buffer/*` and `macro/conf/storage/throughput_write_behind/*` — and
the `F20-conf` capture of 2026-08-22 came back flat: **1.01× between the fastest and slowest arm of
each**, across a four-fold sweep of the buffer size (32Ki through 1Mi) and a sixteen-fold sweep of
the queue depth (1 through 16). Both fail the *Real?* gate — their arms' observed run intervals
overlap — so there is no evidence either setting changes anything at all on this path. The argument
from reading `map.rs` predicted a flat sweep and the sweep is flat, which is as close as a sweep
gets to reproducing a wiring defect.
[Configuration and what each setting is worth](../performance/configuration.md) says so on the page
rather than leaving a reader to conclude the device does not care.

**Fix direction:** thread `&self.conf` into both branches of `get_active_writer` the way
`new_writer` already does. The cached-handle branch is the awkward one — it builds a writer from a
`dup()`ed handle it did not open, so the configuration has to reach it from `self` rather than from
the call site, which it can. Decide separately whether the staging writer at `:224` should be
configured or should stay on glommio's defaults deliberately; it writes a whole map in one pass and
is not obviously the same kind of write. Then re-run `--group conf/storage` against `F20-conf` as
the before, and the two sweeps should stop being flat — which is also the test that the fix did
anything. That is nineteen minutes of machine time, not an afternoon.

### 72. An arm with no writes is reported as an arm that ran once

`shoal-bench/src/render/tables.rs:483-493`, `interval`

The *write p50 across runs* column of
[Configuration](../performance/configuration.md#every-arm) reads `one run` for every `r100` arm of
the shard and memory sweeps. All eleven of them ran five times — `F20-conf` records
`runs: 5` and five wall clocks for each. They have no *writes*, because `r100` is a pure read share.

```rust
// tables.rs:486 - `interval`
match arm.capture.stat_interval_ns(op, metric) {
    Some((low, high)) => format!(...),
    None => "one run".to_string(),
}
```

`stat_interval_ns` (`model/macro_layer.rs:351`) returns `None` for two unrelated reasons: fewer than
two runs, and `run.ops.get(op)?` failing because the arm never performed that operation. The caller
collapses both into the sentence that describes only the first. The neighbouring `write p50` and
`write p99` columns get this right and print `–`, so one row says the arm has no writes twice and
then says it ran once.

It is a wrong sentence rather than a wrong number, which is why it is filed here rather than
mattering to a verdict — but it is a statement about provenance on a page whose whole argument is
that a difference counts only when the intervals are disjoint, and a reader checking whether an arm
was measured enough times is told it was not.

**Fix direction:** distinguish the two `None`s. The narrow version is for `interval` to ask whether
the op exists before asking for its spread, and return `–` when it does not. The version that stops
this recurring is for `stat_interval_ns` to return something with three cases rather than an
`Option`, since every caller that formats it has the same choice to make and two of the three
call sites (`tables.rs:59`, `:492`) already make it independently.

### 73. The hotpath layer's artifact is shared between the workloads that write it

`shoal-bench/src/run/plan.rs:461-482`, `build_plan`

**Half of this is fixed.** The stage layer had the same defect and is
[resolved](resolved/stage-artifact-overwrite.md) under this number; what stays here is the hotpath
half, which the fix deliberately did not take.

The hotpath phase loops over the workloads that opted into attribution and directs every one of
their profiles to the same file:

```rust
// plan.rs:466 - the hotpath phase
for id in instrumented_for(inputs, Layer::Hotpath) {
    steps.extend(wipe_steps(inputs));
    steps.push(Step::Command(workload(
        inputs,
        id,
        vec![/* ... */],
        Stdout::LastLine(artifact(inputs, Layer::Hotpath)),
    )));
}
```

A second profiled workload would overwrite the first, and the artifact that resulted would be
indistinguishable from a correct one — right schema, plausible scopes, and silently describing one
workload where it claims to describe the capture.

Nothing reaches it today: `PROFILED_WORKLOADS` holds one workload, and it stopped being the list the
stage layer reads, so growing the stage layer no longer grows this one. That is why this is filed
rather than fixed — it is latent in exactly the way the stage half was, and the moment somebody adds
a second hotpath workload it is live.

**Fix direction:** the same three pieces the stage half took, and
[Resolved #73](resolved/stage-artifact-overwrite.md#the-fix) has them written down. A scratch path
per workload; a `HotpathProfiles` artifact keyed by workload, with the current single-profile shape
accepted as version 1 so the committed captures keep rendering; and `collect::hotpath` folding the
scratch files rather than only checking one. The awkward part is that a hotpath profile arrives as
the last line of stdout rather than as a file the run writes, so `Stdout::LastLine` has to take a
per-workload path and the collector has to parse each one.

### 75. A control that was measured at every width is not drawn, and the caption says it is

`shoal-bench/src/render/chart/micro_scaling.rs`, the width series selection

[Micro benchmarks](../performance/micro.md#how-the-cost-grows-with-the-width-of-one-row) opens its
width chart by naming its own control:

> A **flat** line is a fixed per-call cost - the header decode is here as exactly that control,
> since eight bytes is eight bytes at every width.

It is not here. `chart-micro-scaling-width` draws eight series and its `aria-label` says so
(`Cost of 8 operations against row width`), while [F22](../features/row-size-benchmarks.md) swept
**ten** `wire_codec/width/*` groups. The two it drops are `request/decode/header` — the control the
caption points at — and `request/decode/access_unchecked`. The growth table underneath the chart has
the same eight rows and the same two omissions.

The measurement itself is fine, which is what makes this a documentation defect rather than a
benchmark one. From `f22-row-size.micro.json`, `request/decode/header` runs 0.7071 ns at 64 B and
0.7088 ns at 64 KiB — a quarter of a percent across a thousand-fold change in row width, which is
exactly the flat control the axis needs to be read against. A reader who trusts the caption
concludes the axis has been validated against a control they can see; a reader who counts the series
finds it has not.

**Evidence: established from the committed artifact**, not from reading the source — the ten swept
identifiers are in `f22-row-size.micro.json` and eight of them are on the page. That is the reverse
of how most entries here were found, and it is why it survived review: nothing compares the number
of series a chart draws against the number its data contains.

**Fix direction:** two choices, and they are not equivalent. Drawing all ten makes the caption true
and costs a reader two more curves on a chart that already has eight — which is the palette's cap
([F19](../features/chart-legends.md)), so ten would need a decision about colour rather than a
selection change. Naming the control in the table but not on the chart is cheaper and keeps the
chart at eight, but then the caption has to say where the control is. The third option is the one to
avoid: quietly deleting the sentence, which loses the fact that the axis *has* a control and that it
holds. This is also the second instance of what [F18](../features/results-pages.md) filed as a
limitation — nothing checks that a page's prose still describes what it draws — after the four
caption drift found by grep in `0851a22`.

### 77. A macro-only capture can never reach the pages it was taken for

`shoal-bench/src/render/page.rs`, `Page::current_for`

**The selection this item was filed against is gone.**
[Resolved #79](resolved/micro-only-capture-current.md) found the same conflation from the other
side — a *micro*-only capture becoming current and emptying the seven macro pages — and resolved the
current capture **per layer**, so a macro-only capture can now become the macro pages' source.
**That is half of what this item asks for and not the half it is about.** What is left is the part
that matters: a page has no way to say *these rows in particular have been re-measured since*, so a
correction that is committed, comparable and newer is still invisible to the reader looking at the
figure it corrects.

The selection it was filed against was this, one label shared by every page:

```rust
// which capture the current numbers come from: the caller's choice, or the most recent one
// that produced a micro layer
let current = match &args.current {
    Some(label) => label.clone(),
    None => timeline
        .iter()
        .rev()
        .find(|snapshot| snapshot.micro.is_some())
        .map(|snapshot| snapshot.label.clone())
        .unwrap_or_default(),
};
```

**A capture taken with `--group` or `--layer macro` has no micro layer**, so it can never be
`current`, however new it is and however exactly it covers the arms a page draws. That is most of
what [F21](../features/benchmark-groups.md) exists to make cheap: `list --groups` prints twelve
sets and prices them precisely so a narrow question does not cost two hours, and
[CLAUDE.md](../../../CLAUDE.md) tells a reader to use a group rather than a prefix. A capture taken
that way lands in `docs/perf/runs/`, appears on the freshness table, appears on
[Every workload](../performance/all-workloads.md) — and is not what any other page reports.

~~**The live instance**, which is how this was found.~~ **Overtaken, and not by a fix.**
`f24-routing` is a full capture taken after F23, so the configuration page now reports these arms
at 22,486/s from a capture that describes the current writer, and the figures below no longer
appear anywhere. The instance is kept because the *mechanism* it describes is untouched: it took a
two hour capture of every layer to dislodge a stale number that a ninety second one had already
corrected, which is exactly the cost this item is about.
[Configuration and what each setting is worth](../performance/configuration.md) reports the
`latency_buffer` sweep at `4Ki @ 64 KiB` as **18,537/s** and calls `256Ki` the best rung at 1.22×.
Those come from `f22-row-size`, whose macro layer the same site marks **stale, four commits**,
because [F23](../features/self-sizing-staging-buffer.md) changed the writer underneath it.
`f23-staging-buffer` measured those exact fifteen identifiers at the current commit on a clean
tree and says **22,619/s** and 1.010× of spread. Both numbers are committed, one is drawn, and it
is the one that describes code that no longer exists.

**Not a wrong number, and that is what makes it bad.** The page is internally consistent, its
provenance line is accurate, and the badge on the overview page does say the capture is stale. The
defect is that the page has no way to say *these rows in particular have been re-measured since*,
so the correction is available, committed, and invisible to the reader who needs it.

**Evidence: established by rendering.** `render` was run on a clean tree at `c46310b` with
`f23-staging-buffer` committed; the fifteen new rows appear on `all-workloads.md` and every other
page is unchanged apart from its provenance line and the staleness badges. Then traced to the
`find(|snapshot| snapshot.micro.is_some())` above.

**Fix direction:** the choice conflates two questions — which capture is the *reference* for the
micro layer's comparisons, and which capture most recently measured *this arm*. The first genuinely
wants a full capture. ~~The second is per-arm and has an answer here.~~ The second turned out to be
**two** questions rather than one: which capture measured this *layer*, which
[Resolved #79](resolved/micro-only-capture-current.md) answered, and which capture measured this
*arm*, which is what is still open. Drawing each arm from the newest
capture that measured it would mix captures within a page, which is exactly what
[Baseline](../performance/baseline.md) forbids and should stay forbidden — so the cheaper fix is to
keep one `current` and have a page *say* when a newer capture covers arms it is drawing, the way the
freshness table says a layer is stale. A row that could be labelled "re-measured in
`f23-staging-buffer`" would have made this visible without joining two machines' numbers into one
table. Filed as work rather than fixed here, in [TODOs](todos.md).


### 81. A stage flag saying a query waited on disk is defined, never set and never read

`StageFlags::loaded_from_disk` (`shoal-core/src/server/stage_profile.rs:110`) is documented as
"whether this query had to wait on a partition being read from disk", and the setter for it is
generated beside every other flag's:

```rust
set_loaded_from_disk(bool) => loaded_from_disk
```

`stage_profile.rs:423`

**Nothing calls it.** A repository-wide search for the name finds the field, its `Default`, the
generated setter, and one integration test whose name happens to contain the words. No query
anywhere sets it, so every stage record ever written says `false` — including the records of
queries that did park on a disk read, which is exactly the population the flag exists to separate.
Nothing reads it either: no report in `shoal-bench` joins on it, so the wrong answer has never
been printed.

That is what makes this worth filing rather than deleting. The stage layer's job is to say where a
query spent its time, and *whether it waited on IO* is the single largest fork in that answer — a
report that pooled parked and unparked gets would put the boundary between two distributions
wherever the mixture happened to fall, which is the same mistake `StageOp` exists to avoid
([Resolved #76](resolved/stage-join.md) is what happens when a stage layer is trusted without
being read back).

**Evidence: established by reading the source**, and by the search above rather than by running
anything — a flag that is never set produces no failure to reproduce.

**Fix direction:** `block_on_load` returning `true` is the moment a query becomes one that waited,
and `PersistentTable::load_partition` releasing it is the moment it stops. The flag belongs on the
`QueryMetadata` that is parked in `blocked` and carried into the replay, which already survives
that round trip — `skip_disk` and `failed` are set on exactly that path
([Resolved #16, 51](resolved/partition-load-failure.md)). Setting it is small; the part worth
doing carefully is the report, which has to treat the two populations as two rather than
subtracting one from the other. Found while fixing
[Resolved #80](resolved/never-flushed-partitions.md), which is about the other side of the same
call.

### 87. A collector that rejects every span is indistinguishable from one that accepts them

`opentelemetry-otlp` 0.28, `OtlpHttpClient::export`
(`~/.cargo/registry/.../opentelemetry-otlp-0.28.0/src/exporter/http/trace.rs:53-71`)

The OTLP/HTTP response body is an `ExportTraceServiceResponse`, whose `partial_success` field
carries `rejected_spans` and an `error_message`. A collector uses it to say *I took the request and
threw the contents away* — the wrong tenant, a resource limit, a malformed attribute. The exporter
never reads it:

```rust
let response = client.send_bytes(request).await.map_err(...)?;

if !response.status().is_success() {
    return Err(OTelSdkError::InternalFailure(error));
}

Ok(())
```

The status is checked and the body is dropped on the floor. So a collector answering `200` with
`partial_success { rejected_spans: 412 }` produces exactly the same result inside Shoal as one that
stored all 412 — `Ok(())`, no `BatchSpanProcessor.ExportError`, nothing at any log level.

**Established by reproduction**, while diagnosing spans that were not appearing in Grafana. A proxy
placed between the example and the collector captured the real answer:

```
sent=178434B tenant='Shoal' -> 200 body=b'\n\x00'
```

`\n\x00` is field 1, length zero: an empty `partial_success`, meaning nothing was rejected. That
run was healthy, and the point is that **the unhealthy case would have looked identical from
inside Shoal** — which is why the failure was chased through Shoal for as long as it was when it
was never there. The 412 is the exact span count of one `cargo run --example tmdb`, counted by
decoding the protobuf the exporter sent.

**Fix direction:** wrapping `SpanExporter` to decode the response is not possible from outside the
crate — `OtlpHttpClient::export` discards the body before returning, so there is nothing left for a
wrapper to inspect. The options are upstream, or a hand-rolled exporter over `opentelemetry-proto`
0.28 (already in the graph, as a dependency of `opentelemetry-otlp`), which would be the third
implementation of an OTLP client in this tree's dependency closure and is hard to justify for one
field. The cheap and honest thing, done in
[Observability](../operations/observability.md#tracing), is to write down that confirming delivery
means asking the collector rather than reading Shoal's logs.

### 91. A compaction that fails after writing ends the compactor

`shoal-core/src/server/tables/storage/fs/compactor.rs`, `FileSystemCompactor::start`,
`JobFailure::Fatal`

~~Every job is `?`. A compaction that fails - an archive it cannot open, a read that comes back
short - returns the error out of the loop, and the compactor task ends.~~ A job that fails
**before it writes** is tried again with a backoff since
[Resolved #91, 107](resolved/compaction-retry.md), which is where every failure seen so far
was. The remainder is the harder half that page names: a job that fails **after** its first
write - a partially written archive, a map entry not yet re-pointed - still ends the compactor
with the error, the next rotation's send into the closed channel ends the shard, and a client
with a query in flight to that shard waits forever ([item 62](#62-a-server-that-has-exited-leaves-its-client-connections-open)).

~~**Established by reading the source.** No failure after the write has been reproduced; the
fault the tests inject is met at the load, before it.~~ **Reproduced** (2026-10-03) by
[F70](../features/storage-faults.md)'s full disk, armed in process over a standalone node's
storage with a 64 MiB budget while 64 KiB rows were written. The compactor's archive write met
`ENOSPC` first and ended the compactor; a rotation's send into the closed channel then logged
`KanalSend(ReceiveClosed)` and ended the shard, and the next write through it had no answer in
thirty seconds:

```text
ERROR shoal_core::server::tables::storage::fs::compactor: error=Fatal(IO(Custom { kind: StorageFull, error: "No space left on device (os error 28), op: Writing path: Some(\".../Blob/archives/…\")" }))
WARN  shoal_core::server::tables::storage::fs: msg="the compactor was gone before shutdown reached it"
ERROR shoal_core::server: error=KanalSend(ReceiveClosed)
```

On a real 64 MiB device the same writes met `ENOSPC` at the intent log first, which refuses the
write by name (`OutcomeUnknown`); which file meets a full disk first is timing. Not fixed by F70,
which built the fault.

**Fix direction:** the "old complete or new complete" rule
[C7](../distributed/failover.md#snapshots-and-atomic-installation) sets for snapshot
installation, applied to the archive write and the map's re-pointing, after which a failed
write can be retried by discarding the incomplete new; and a rotation that finds its compactor
gone should say so rather than end the shard.

### 117. A restore under the fixture suite's load finds its target table not yet empty

`single_node_data_has_a_verified_cluster_migration_path` fails under the fixture suite at six
threads, about one run in two here, and passes alone. A group's restore comes to:

```text
{"Failed":{"reason":"2a02ce9f-…/0 holds 117 rows of the group's tablets; a restore is into
an empty table, and this one is not"}}
```

The emptiness check is the `Loading` phase's scrub in `server/shard/restore.rs`. The table was
new, and the only rows that could be in it are the restore's own. **Suspected, from reading the
source and not reproduced:** the step is driven again after a leader change. The group commits
`Loading`, loads rows, and loses its leader before committing past `Loading`. The next leader
resumes at `Loading` and scrubs rows the first attempt put there. The scrub needs to tell those
rows apart from rows that were there before the restore, for example by the restore's own
boundary. It must not skip the check on a resume, because the check is what refuses a
non-empty target.

**Established by running it**: the run for [Resolved #15](resolved/shard-mesh-admission.md)
failed it first, and the loaded fixture runs for
[Resolved #115](resolved/retry-sidecar-crash-window.md) failed it once in two.

### 119. `down_retains_placement_during_grace` kills a member before the third voter is promoted

The test starts three nodes and kills one without waiting for three voters
(`cluster.wait_voters(0, 3)`, which the M3 tests call). Under the workspace run at six threads
the third node was still a **learner** when the kill landed. The control group then had two
voters, one of them dead, and could not commit the verdict. The detector's own read had the
dead member at `phi` 300 after thirty misses, while `MEMBERS` kept it `up`:

```text
the dead member was never called down: {… "learners":["c21edec1-…"], …}
detector: {… "3c3efa4a-…":{"misses":30, …}, … "phi":300.0, "since_last_ms":30175.8, …}
```

The detector and the commit are right. The test's schedule is wrong: it asserts a verdict that
needs a control quorum, and its kill can land before that quorum exists. The fix is to wait for
three voters before killing. **Established by running it**: it failed once in the
whole-workspace run for [Resolved #115](resolved/retry-sidecar-crash-window.md) and passed
alone at once.

### 203. `pending_bytes` bounds a group, and the configuration's own comments call it a shard's

**Low: a comment that states the wrong scope, with a consequence for sizing a node.**
`cluster.replication.pending_bytes` is checked for one group at a time. `propose_write`
(`shoal-core/src/server/shard/groups.rs:2073`) compares the proposing group's own count with
the setting:

```rust
if group.pending_bytes + bytes > cluster.replication.pending_bytes {
```

The book says so: "proposed and unanswered bytes one shard holds per group"
([Configuration](../getting-started/configuration.md)), "Bounds are per group and definite"
([F40](../features/replication.md)). The configuration struct says otherwise, twice, in the
comments a reader of the code meets first (`shoal-core/src/server/conf/cluster.rs:471`,
`:564`): its default is "The default bound on bytes proposed and not yet answered, per shard",
and the field is "The most bytes a shard holds proposed and unanswered before it sheds a
write".

The consequence is the reason to file it and not only to correct two comments. What a shard may
hold proposed and unanswered is the setting times the groups it hosts, and nothing sums or caps
that: the sum is computed for a status report (`groups.rs:3307`) and compared with nothing. At
the default of 64 MiB, a shard hosting a dozen groups may hold 768 MiB of proposals, which is
not what somebody sizing `node_memory` from the comment would expect. Every table adds groups,
and a bucket would add two tables' worth ([S2](../object-storage/buckets.md#what-it-costs)).

**Established by reading the source**, while planning object storage.

**Fix direction:** correct the two comments, and decide whether a shard needs a bound of its own
over the sum. If it does, it is one more comparison in `propose_write`.

### 204. The stream budget is built for a shard and documented for a node

**Low to medium: a budget that is larger than every page says it is.**
`cluster.migration.stream_bytes_per_sec` is described as one token bucket for a node. The
configuration's comment is "One bucket per node, not per device"
(`shoal-core/src/server/conf/cluster.rs:836`); [C8](../distributed/rebalancing.md#transfer-budgets)
says "one token bucket per sending node across every stream and group"; the
[configuration page](../getting-started/configuration.md), the [glossary](glossary.md) and
`CLAUDE.md` say the same.

The bucket is built once for each shard. `ShardNetwork::new`
(`shoal-core/src/server/replication/network.rs:856`) makes a `RateLimiter` from the whole
setting, and each shard's start passes it the whole setting
(`shoal-core/src/server/shard.rs:4149-4151`, "the byte budget every stream this shard sends
draws on"):

```rust
limiter: RefCell::new(RateLimiter::new(stream_bytes_per_sec)),
```

The limiter's own comment has it right, "One per shard network" (`network.rs:487`), and so
does one sentence of [F46](../features/capacity-rebalancing.md#budgets-and-the-reserve),
"Every stream a shard sends draws chunks from one token bucket", further up the same page
than its "One bucket per node, not per device".

So a node of six executors may send six times the setting: 384 MiB/s at the default of 64 MiB/s,
where its operator set 64. The bound that was meant to keep a move's cost away from the
foreground is as loose as the node has cores. On the lab it is hidden by the network, since
1 GbE carries about 117 MiB/s whatever the budget says.

**Established by reading the source**, while planning object storage, whose recovery wants a
budget for each device and had to learn first what the existing one is
([S10](../object-storage/recovery.md#what-exists-today)). It has not been measured.

**Fix direction:** one of two, and they are different decisions. Share one bucket between a
node's shards, which makes the pages true and needs a bucket that more than one executor can
draw on. Or divide the setting by the executor count where it is passed, which keeps a bucket
for each shard and makes the node's total what was asked for, at the price of a shard that
sends alone being held to a fraction of it. Then say on every page above which it is.

### 205. A map whose only change is the control leader is published, and dropped by every shard

**Low: a stale name, as far as it was traced.** `ControlPlane::publish`
(`shoal-core/src/server/control/plane.rs:1300-1304`) rebuilds the tablet map from the applied
state and pushes it when its version or its leader has moved, and says why in a comment:

```rust
// the leader is not part of the version, so a leader change alone still goes out
let moved = map.version != self.map.version || map.leader != self.map.leader;
```

Every shard then refuses it. `Shard::install_map` (`shoal-core/src/server/shard.rs:1876`)
returns as soon as `MapCell::install` declines, and `MapCell::install`
(`shoal-core/src/server/map.rs:1237-1244`) ignores "one at or below the installed version":

```rust
if map.version <= inner.version && inner.version != 0 {
    return false;
}
```

A map with the installed version and a new leader is at the installed version. So a change of
control leader that commits nothing else reaches no shard's map, and since `push_topology` is
the last thing `install_map` does (`shard.rs:1924`), no subscribed client is pushed it either.
Each goes on naming the old leader until something else moves the version: a member's health,
a plan's step, a table's policy.

[C4](../distributed/tablet-map.md#how-a-map-reaches-a-shard-and-a-client)'s diagram shows both
halves, "version or leader moved" on the control thread and "only a newer version" on the
shard, without remarking that the second undoes the first whenever only the leader moved.

In most leader changes something else does move the version, since the old leader is usually
called down. The case that leaves the name stale is an election that unseats a leader which
stays up.

What reads the stale name was not traced. `TabletMap::frame` copies `leader` into the frame a
client is pushed, and `shoaladm` holds a leader in its model of a cluster, which `upgrade`
uses to restart the leader last. Whether that model's leader comes from a pushed frame or
from an admin read was not established, and that is what decides how much this matters.

**Established by reading the source**, while planning object storage, whose pool map would be
pushed the same way ([S5](../object-storage/placement.md#the-pool-map)). It has not been
reproduced. The reproduction is a fixture test that moves the control leadership between two
members that both stay up, and reads `TOPOLOGY` from a shard of a third.

**Fix direction:** let a shard take a map of the installed version whose leader differs,
replacing the cell and pushing the topology, and skipping the rebuild of rings and groups
that nothing but a version change needs. The alternative, moving the version on a leader
change, makes every control election a new map version for every shard and client to install.

### 206. Forty-two links point at headings that are no longer there

**Low: documentation drift.** A link to a heading is spelled from the heading's text, and a
heading changes when its item is resolved, renamed or rewritten. `mdbook build` checks that a
linked page exists and does not check the anchor, so such a link goes on building and drops
its reader at the top of the page, or at the top of a page the item has left.

Forty-two do, on twenty-eight pages. Fifteen targets account for all of them:

| Target that no longer exists | Links | What became of it |
| --- | --- | --- |
| `optimizations.md#o2-every-returned-row-is-copied-at-least-twice` | 16 | The heading gained "the resident half is done" and then "done", and its id with them |
| `known-issues.md#54-shoaldb-needs-three-crates-the-caller-has-never-heard-of` | 5 | Resolved as [macro-emits-three-crates](resolved/macro-emits-three-crates.md); the number's open remainder has another heading |
| `known-issues.md#16-panics-on-the-hot-path` | 3 | [Resolved](resolved/hot-path-panics.md) |
| `known-issues.md#180-a-first-write-queued-past-a-groups-identity-memory-is-refused-identityexpired` | 3 | On the [resolved page](resolved-issues.md) |
| `known-issues.md#17-leftover-debug-printlns`, `#20-orphaned-source-files`, `#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool` | 2 each | On the resolved page |
| `known-issues.md#130-…` and `#131-…`, from one page | 2 | [Resolved](resolved/stream-connection-accounting.md) |
| `known-issues.md#38-…`, `#80-…`, `#99-…`, `#193-…` | 1 each | On the resolved page |
| `tables/row-size.md#what-it-settled--five-of-six-ran` | 1 | The heading now says six of six |
| `operations/shoalctl.md#compiling-it-for-your-schema` | 1 | The heading gained "by hand" |
| `direction/connection-pool.md#benchmark` | 1 | No heading of that name remains |

The pages that hold them are under `appendix/` (seventeen links), `features/` (ten), `tables/`
(six), `direction/` (four), `cluster-testing/` (two), and one each under `api/`,
`architecture/` and `distributed/`.

**Established by running a check**: the book was built, the ids on every built page were
collected, and every relative link in `docs/src` was resolved against them. The check was a
throwaway script written to verify the links of the
[Object Storage](../object-storage/overview.md) part, where every link resolves; it was not
kept. Seven more dangling links stood on the pages that change touched, and were retargeted
there.

**Fix direction:** retarget the forty-two, which is mechanical since each item's resolved
page is named in its row of [Resolved Issues](resolved-issues.md). Then keep them from coming
back: a test beside `shoal-bench/tests/acceptance_tables.rs` that resolves every link against
the ids `mdbook` would give each heading, or a link checker run where the book is built. The
rule that a number moves to the resolved page is what breaks these links, so it will go on
breaking them.

### 219. The tablet model's saved schedules are not what its regeneration writes

`shoal-model/examples/regenerate_schedules.rs`, `shoal-model/schedules/*.json`

**Low: drift in a test's inputs, not a wrong answer.** `cargo run -p shoal-model --example
regenerate_schedules` is what CLAUDE.md says to run after a model change, and run on today's tree it
rewrites seven of the tablet model's eight saved schedules: each gains the `strong_read` weight
[F41](../features/read-consistency.md) added to the generator's parameters, and the generated ones
come out as other runs, since a weight more changes every draw. The files committed are older than
the weight. Nothing fails: the tests only load the files and replay them, and each still replays to
the violation it records (`protocol_model.rs`). But the files are not what their builder makes,
which the stripe model's tests hold their own schedules to and the tablet model's do not.

**Established by running it**, while the contract was agreed on 2026-10-09
([the record](../object-storage/contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)),
on a tree that changed nothing in the tablet model; the rewritten files were put back.

**Fix direction:** regenerate them and commit the result, checking that each still records the
violation it is saved for, and add the stripe model's test that a file is byte for byte what its
builder makes, so the next weight added cannot drift past it. A new weight that should leave old
schedules as they were is given a serde default and skipped when at it, as the stripe model's are.

### 222. A second forward of one bundle on a peer connection adds its entries and not its bytes

`shoal-core/src/server/peer/listener.rs`, `Inflight`

**Low: a figure undercounted, not a wrong answer.** A node serving forwards counts what each peer
connection has in flight, by bundle (`Inflight.bundles`), to bound it. A second `Forward` frame of
a bundle already in flight on the same connection adds its entries to the bundle's count and not
its bytes, so the bound reads the connection lighter than it is. Before
[F74](../features/client-routing.md) a node forwarded a bundle at most once to a peer; a client
routing by topology sends one bundle as several runs, and a node whose map is stale for a moment
can forward two of them to the same peer.

**Established by reading the source** while checking that nothing on the server keys state by
bundle alone, which the runs depend on. Not reproduced.

**Fix direction:** key the in-flight record by the bundle and the frame, or sum both counts when
a bundle is seen again.


### 224. A strong read on a healthy cluster under load is refused because the leader's heartbeat round found no quorum

`shoal-core/src/server/shard/reads.rs`, `read_barrier`

**Medium: a read refused that a healthy cluster should serve.** A read at `Quorum` asks its
group's leader for a barrier: openraft's `get_read_linearizer(ReadPolicy::ReadIndex)`, which
confirms the leader's term with a heartbeat round to its voters. When the round comes back short
the read is refused `QuorumUnavailable` - "could not confirm its leader: not enough for a quorum"
on the leader's own shard, "the leader of group … could not confirm its term" through a hop. On the
lab, with all three members up, the round came back acknowledged by the leader alone
(`got: {…/5}`) in bursts through an arm: retriable, but a bench with no retries counts each as a
failure, and a caller with a budget pays a round trip for it.

**Measured on the lab** by [F74](../features/client-routing.md)'s `Quorum` pass, `read100` through
the bench's three clients on europa, titan and hyperion at a factor of three, performance governor:
3 of the 16 runs at a bundle of 64 (256 queries in flight a client) met it, on both sides of the A/B, so
not from routing - 235 of 533,126 reads routed at `77f37df`, 260 of 555,408 through the endpoints
and 1,969 of 548,973 routed at `1ae254b`, the last in nine bursts over the arm's 25 seconds. No run
at a bundle of 1 or 16 met it. Not reproduced in a test; why the followers' acknowledgements were
late, and whether openraft's round waits as long as a heartbeat interval or less, are **not
established**.

**Fix direction:** read the round's own timeout in openraft and size it against the heartbeat
interval the groups run at; failing that, retry a short round once on the leader before refusing,
since a refusal costs the client a round trip and a retry costs the leader one heartbeat.

### 226. `compare` refuses two captures whose nodes' memory differs by kilobytes

`shoal-loadgen/src/compare.rs`, `facts`

**Low: a comparison refused that should be made, with a waiver to make it.** `compare` refuses
two captures that differ in a fact that changes a capture's numbers, and the `nodes` fact is each
node's cpu, cores, governor and `memory_bytes`, compared as text, so to the byte. A host's
`memory_bytes` is its MemTotal, which moves by kilobytes when the kernel does: what the kernel
reserves for itself changes with its version. Two captures of the same benchmark on the same three
hosts either side of a kernel update are therefore refused, though no node has another amount of
memory in any sense a benchmark can see. The `driver` fact leaves memory out, so the driver's host
is not held to it, and the two facts disagree about what makes a machine the same.

**Established by running it**, by [F78](../features/object-harness.md#performance)'s lab check on
2026-10-11: F69's capture from 2026-10-03 and one of the tree at `064a2d1` were refused on `nodes`
alone, europa's MemTotal 16 KiB less (47,002,193,920 bytes, then 47,002,177,536, across 7.0.0-31
to 7.0.0-34) and titan's 8 KiB more; spec, schema, inventory and dataset were the same, and with
`--allow nodes` every arm joined.

**Fix direction:** compare memory at a granularity that names a machine, not a boot - whole GiB,
or within a percent - in `facts`, and print the exact figures beside it as now; the `driver` fact
could then carry memory too. A waiver stays the way to compare across a real change of memory.
