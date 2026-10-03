# S1. What has to exist first

**Nothing on this page is built either.** It is the list of what Shoal has to gain before any
object storage code is written, in one place, so that no page after it has to rediscover a
gap and no milestone starts on top of one.

A prerequisite here is a **gap in Shoal as it is today**: work that would land in a change of
its own, before any object storage code. What the object store itself has to do is not a
prerequisite, however hard it is; that is on its own page and in the
[contract](contract.md#the-contract). The line is drawn that way so that this list stays
something that can be finished.

## The rule the labels follow

Every row is labelled, and the label follows one rule.

- **Required**: built without it, the feature would be incorrect or unsafe, or would later
  need rework to a format that is persisted or a protocol that is on the wire.
- **Optional**: it can be added later without changing anything already built, or it is
  needed only under a design answer that has not been chosen.

The rule is deliberately not "would be nice first". An item that only makes something faster
or larger later is optional, because skipping it costs nothing that has to be undone. An item
whose absence would be baked into a key, a file or a frame is required, because the cheapest
day to do it is the day before the format exists.

**No object storage code is written on top of a required prerequisite that is outstanding.**
[Milestones](milestones.md) places each required row no later than the start of the first gate
that needs it: it lands as a change of its own, with its own page and its own tests by the
repository's rules for a fix or a feature, before any of that gate's object work begins. The
last column below says which gate, and nothing stops a row landing sooner
([The order](#the-order)). This is the decision of 2026-10-02 written as a rule: the feature
is built right the first time, and nothing required is skipped to reach a gate sooner.

## Gaps in Shoal today

### Required

| Prerequisite | What exists today | What is needed | Why it is required | Lands no later than the start of |
| --- | --- | --- | --- | --- |
| ✅ **A conditional write on unsorted tables, with a typed refusal**: delivered by [F68](../features/conditional-writes.md), for sorted tables too | ~~The unsorted queries are insert, get, delete, update and exists (`shoal-proto/src/shared/queries/unsorted.rs:14-26`). An insert replaces whatever is there. An update names a key and new values and nothing it expects to find (`:186-191`), and succeeds whenever the row exists (`shoal-core/src/server/tables/persistent/unsorted.rs:1240`). A result is `CommandResult { kind, ok }` and a refusal is a `String` (`shoal-core/src/server/replication/types.rs:53-58`, `:77-91`)~~ Since F68 an insert, delete or update on either table kind can carry a `WriteCondition` (`Absent`, or `Matches` the table's own filter), judged at apply in committed order, refused as `ConditionRefusal::RowExists`, `RowMissing` or `RowMismatch` (`shoal-proto/src/shared/queries/condition.rs`), carried in `ResultKind::Refused` and remembered with the request's identity. A replicated one needs wire version 7 activated | A write applied only if the row is as its writer expects (a field equal to a value, or no row at all), judged at apply in committed order as every result already is, and refused with a reason a caller can branch on | A stripe's commit has to be refused when its row has moved under the writer. An unconditional one lets a parity computed from an old state overwrite a new one, and nothing notices until a degraded read ([S7](write-path.md#what-breaks-without-the-condition)). Path identity and the truncate epoch rest on the same primitive ([S3](objects.md)) | [M12](milestones.md#m12-tables-what-the-metadata-needs) |
| ✅ **Known issue 198: a partition key of two fields does not compile**: resolved with item 92 by [Resolved #92, #198](../appendix/resolved/composite-partition-key.md) | ~~The derive's branch for more than one partition field fails its own expansion, and the issue names the fix: hash the fields one at a time (item 198). Item 92 is the same defect, filed earlier~~ Since Resolved #92, #198 a row hashes its partition fields one at a time in declaration order (`shoal-derive/src/traits/partition_key.rs`), the bytes its key's tuple hashes to. Every query kind reaches a composite key on all four table kinds, two composite shapes are frozen in the golden key set (`shoal/tests/partition_keys.rs`), and the TMDB schema holds a table keyed by three integers on the lab. SHQL still cannot name one ([item 41](../appendix/known-issues.md#41-shql-cannot-express-a-composite-partition-key)) | That fix, which closes both | `StripeMeta`'s key is a consumer id, an object id and a stripe index. Packing the three into one field to step round a derive defect would freeze the workaround into the persisted key of every stripe ever written. The fix is small and already stated | [M12](milestones.md#m12-tables-what-the-metadata-needs) |
| ✅ **Known issue 46: an unmarked directory is claimed, not refused**: resolved by [Resolved #46](../appendix/resolved/unmarked-directory-refused.md) | ~~A storage root with no `shoal-meta.json` is taken as one nothing has written to (item 46). Every distinct root is locked and carries a mirror of the marker since [Resolved #43](../appendix/resolved/marker-every-root.md)~~ Since Resolved #46 `StorageMeta::claim_roots` (`shoal-core/src/server/meta.rs`) sorts every root into empty, marked or somebody's files before it writes any: an empty root is claimed, a marked one is held to its marker, files with no marker are refused by name. The primary lists the roots it mirrored onto, so one of them found empty is refused as wiped while a root just added is mirrored, and `<node> claim` marks every root | A claim that tells an empty directory from a marked one and refuses anything else | A device and each of its slices are claimed the way a storage root is. A replaced disk mounted at the old path is an empty directory; taken for the old device, its slices hold none of the stripe chunks the rows call current ([S4](pools-and-devices.md#a-device-has-slices)) | [M14](milestones.md#m14-devices-and-pools-on-one-node) |
| **A failure domain on a member** | `MemberRecord` carries addresses, slots, weights and wire facts, and nothing about where the node stands (`shoal-core/src/server/control/types.rs:85`). "A member has no domain and the planner spreads by node alone" ([todos](../appendix/todos.md#distribution), what F46 left undone; [C15](../distributed/open-issues.md#filed-as-unbuilt)) | A failure domain a node reports and the control group commits, which placement reads | [P11](contract.md#the-contract) counts stripe chunks in distinct failure domains. Without one, two chunks of a stripe can sit on one host, and one host lost is two chunks lost | [M14](milestones.md#m14-devices-and-pools-on-one-node) |
| **Free bytes reported for every storage root** | A node reports one figure: `statvfs` of the default latency path (`shoal-core/src/server/control/capacity.rs:33-55`), carried as `StatusReport::free_bytes` (`shoal-proto/src/shared/protocol/peer/control.rs:229`) | Free bytes for each root a node writes to, in the report and in the leader's view | Placement and the reserve check are judged for each device. With one figure a node, a full disk is found by a write that fails after its commit ([S5](placement.md), [S10](recovery.md)) | [M14](milestones.md#m14-devices-and-pools-on-one-node) |
| **More than one frame for one query on the client wire** | A request body is read whole into one allocation before anything is routed (`shoal-core/src/server/request_body.rs:55-74`). A frame is bounded at 64 MiB (`shoal-proto/src/shared/protocol.rs:154`), which a client's hello hard-codes (`shoal-client/src/client.rs:558`). A response is one frame a query. `Flags::LAST`, "the last one for its query", is reserved and unused (`protocol.rs:351`) | A write carried in, and a read answered by, a sequence of bounded frames | R13 cannot be met by buffering an object. It is a framing change, and a framing change is cheapest before anything depends on the other shape ([S12](wire-and-client.md)) | [M13](milestones.md#m13-the-wire-and-the-baseline) |
| ✅ **A byte bound on an append batch**: resolved by [Resolved #202](../appendix/resolved/append-batch-bytes.md) | ~~openraft sends up to `max_payload_entries` entries in one append, 300 by default (`openraft-0.10.0-alpha.34/src/config/config.rs:67`), and `group_config` sets no other (`shoal-core/src/server/shard/groups.rs:4430`). The request is framed against the frame bound, and one past it is reported `Unreachable` (`shoal-core/src/server/replication/network.rs:406-422`). Filed as item 202~~ Since Resolved #202 `GroupStore` overrides openraft's `limited_get_log_entries` and cuts every batch at `cluster.replication.append_batch_bytes` (8 MiB by default) of log frames, always one entry; the bound is validated against the frame and the replication queue. One entry larger than a frame is still unsendable ([item 208](../appendix/known-issues.md#208-a-write-that-fits-a-client-frame-can-make-a-log-entry-no-peer-frame-carries)), which an inline object's threshold sits far below | A batch bounded in bytes as well as in entries | An inline object is a row. Three hundred rows of 256 KiB are 75 MiB, so a replica that fell behind a run of small objects could not be fed. Wide rows have the same exposure today | [M12](milestones.md#m12-tables-what-the-metadata-needs) |
| **An engine walk of one tablet's rows that a driver can ask for** | The engine enumerates a tablet's keys for a snapshot and for a scrub (`archived_cut`, `shoal-core/src/server/tables/storage.rs:1010`; `canonical_cut` and `snapshot_partitions`, `shoal-core/src/server/database.rs:373`, `:389`). No query can: a `WHERE` on the partition key is mandatory ([SHQL](../api/shql.md)) | That walk offered to a driver on the leading shard, by tablet and by key range | Backfill of a slice that was away too long, a light scrub, and reclamation of a deleted object's stripes all have to enumerate stripe rows ([S10](recovery.md), [S11](scrub.md)). Backfill is the first to need it | [M16](milestones.md#m16-recovery) |
| **Torn-write, full-disk and device-loss faults in the fixture** | The fixture can exit a process at a named line (`shoal-core/src/server/replication/install.rs:217`), fail a table's intent log write (`shoal-core/src/server/tables/storage.rs:900`) and damage an archive record ([F44](../features/repair.md#what-the-fixture-can-do-now)). [C15](../distributed/open-issues.md#filed-as-unbuilt) files disk-full and torn-archive-write faults as unbuilt | A torn write, a full disk and a lost device, for a directory a test names | [P7](contract.md#the-contract) names all three. A clause no test can violate is not checked ([S16](testing.md)) | [M11](milestones.md#m11-step-0-the-harness-and-the-facts) |
| **Operation kinds beyond read and insert, and byte counters, in `shoal-loadgen`** | Read and insert are written into the mix (`shoal-loadgen/src/spec.rs:21-28`), the operation kind (`window.rs:20-25`), the picker (`pick.rs:22-35`) and the table source (`feed.rs:305`). A window counts operations and no bytes (`window.rs:80-91`) | Kinds a schema's buckets can add, and bytes counted where they are sent and received | R15 asks for it by name, and every gate's evidence is a measurement the driver cannot take today ([S15](performance.md)) | [M11](milestones.md#m11-step-0-the-harness-and-the-facts) |

### Optional

| Prerequisite | What exists today | What it would add | Why it is optional |
| --- | --- | --- | --- |
| **D7, client routing by topology** | A client is pushed every topology version and routes by none ([D7](../direction/shard-aware-routing.md)) | A client that picks the node holding what it wants | Only a client that writes or reads stripe chunks itself needs it. A node coordinates in every design here, so nothing built changes when D7 arrives, and D7's own rule is to measure the hop first ([Q15](contract.md#questions-to-answer)) |
| **`Cancel` on the client wire** | Reserved as message type 12 and unwired (`shoal-proto/src/shared/protocol.rs:202`; [todos](../appendix/todos.md#cancel-and-what-it-would-actually-buy)) | A reader abandoning a range it no longer wants | With bounded ranged frames a reader stops by not asking for the next range; what a cancel saves is the tail of one. The type is reserved, so wiring it later changes nothing already on the wire |
| **A clone call in the glommio fork** | `copy_file_range_aligned` exists (`glommio/src/io/dma_file.rs:574`); no `FICLONERANGE` | Splicing a staged range into a stripe chunk without copying it | Needed only if [X6](spikes.md#x6-the-device-store-on-ssd) picks that way of applying an update |
| **Handing an accepted connection to another executor** | Every shard accepts on the shared port, and a frame naming a slot is handed to the executor hosting it (`shoal-core/src/server/peer/listener.rs:181`). Nothing moves a connection | Bytes read by the executor that owns the slice they are for | Needed only if [X11](spikes.md#x11-streamed-bodies) finds the hop between executors too dear |
| **A failure domain above the host** | None | Stripe chunks spread over racks | No deployment has a rack to name. The member's field is a list from the start, so a level is added without a format change |
| **Paging the archive map** | The map holds an entry for every row in memory, about fifty bytes each, and nothing evicts it ([todos](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing)) | A bucket larger than memory allows | It is a ceiling, near twenty million objects a GiB of memory a replica, and not a correctness matter. It lifts inside the table engine without touching an object format. [S3](objects.md#what-it-costs) states it as a limit |
| **Authorization** | Tables have none: an authenticated principal can read and write any table ([todos](../appendix/todos.md#per-table-authorization)) | A principal held to some buckets | A bucket is no less protected than a table is. It should be built once, for both |
| **Known issue 93: the archived hash of a string key** | `get_partition_key_from_archived_insert` hashes a string without the terminator the live path writes, so the two disagree for every string key. Nothing calls it ([item 93](../appendix/known-issues.md#93-the-archived-partition-hash-disagrees-with-the-live-one-for-every-string-key)) | The function deleted, or made to agree and frozen beside the live hash | `ObjectMeta` is keyed by a path, a string, so it is the table this would hit. But the function has no caller and this design adds none, and either fix changes no key that is persisted, so it can be done at any time. It is listed so that nobody gives it a caller first |

## Dependencies to choose

Not gaps in the code, but nothing can be built without them, so both are **required**. Neither
is chosen here.

| Dependency | What exists today | Chosen by |
| --- | --- | --- |
| **An erasure coding crate** (R11) | None. No erasure coding crate is in `Cargo.lock` | [X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs), which compares code families and crates, before [M18](milestones.md#m18-erasure-coding). The candidates are pinned on [S18](contract.md#decision-record) |
| **A checksum with a frozen definition** (R18) | gxhash 2.3 is the one hash in the workspace and is already pinned "as a persistence format" (`Cargo.toml:29-34`), a pin that exists because two majors once disagreed ([Resolved #65](../appendix/resolved/gxhash-pin.md)). `crc32fast` is in the lockfile through other crates' dependencies and nothing in Shoal calls it | [X5](spikes.md#x5-checksums), before [M13](milestones.md#m13-the-wire-and-the-baseline), since a frame that carries a unit's checksum fixes it on the wire. Keeping gxhash is a possible answer; so is a CRC, whose definition no crate's release can move |

## What the lab needs fitted

The lab is europa, titan and hyperion, the hosts of `tmdb_cluster.yaml`
([cluster testing](../cluster-testing/overview.md#the-lab)).

| Item | Needed | Why |
| --- | --- | --- |
| **Rotational disks** | Yes, for [X7](spikes.md#x7-the-device-store-on-hdd), [X12](spikes.md#x12-recovery-and-scrub-rates) and [M19](milestones.md#m19-rotational-devices). One in a Zen1 host is the least that answers X7; two in each host let a 4+2 layout run over real devices | R16 cannot be judged on an emulated disk. A fixed delay has no seek in it, so it ranks an append to a journal and a random write in place the same, and that ranking is what the spike is for |
| **An XFS filesystem** | Yes, for [X6](spikes.md#x6-the-device-store-on-ssd) | The lab's devices are ext4 on titan and hyperion and btrfs on europa. The book's own guidance is XFS, and btrfs is recorded as a poor host for this write path ([Storage Overview](../storage/overview.md#limitations)) |
| **A link above 1 GbE** | No | Loopback on europa measures what a core and a protocol cost, which is what the spikes decide. Only throughput across hosts above about 117 MiB/s is out of reach, and every such number is labelled |
| **A fourth host** | No | Three hosts allow only 2+1 at a host failure domain. A device failure domain, and the fixture on one host, stand in for wider layouts |

## What is not on this page

Obligations of the design itself, each on the page that owns it:

- a lane for object bytes between nodes, and a memory budget for bytes that are not rows:
  [S13](isolation.md);
- the record of which slices missed which writes, and how it survives a checkpoint and
  reaches a new replica: [S10](recovery.md);
- path identity under a key that can collide: [S3](objects.md#path-identity),
  [P14](contract.md#the-contract);
- what a schema change, a backup and a restore mean for a cluster holding object bytes:
  [S14](operations.md#a-schema-change-a-backup-and-a-restore),
  [Q31](contract.md#questions-to-answer).

## The order

| Could start today | Waits on |
| --- | --- |
| ~~Items 198, 46 and 202;~~ The fixture's faults; the driver's kinds and byte counters. None depends on an open question, and each is worth having with no object store at all | — |
| ✅ Item 198, with item 92, resolved by [Resolved #92, #198](../appendix/resolved/composite-partition-key.md) | Done |
| ✅ Item 46, resolved by [Resolved #46](../appendix/resolved/unmarked-directory-refused.md) | Done |
| ✅ Item 202, resolved by [Resolved #202](../appendix/resolved/append-batch-bytes.md) | Done |
| ✅ The conditional write, delivered by [F68](../features/conditional-writes.md) | ~~Nothing, for a condition on one field.~~ Done, for equality on any of a row's filter fields. [Q25](contract.md#questions-to-answer) settles whether the generated rows need more, such as a comparison other than equality |
| The tablet walk | [Q17](contract.md#questions-to-answer), which says what a driver asks it for |
| The failure domain and free bytes for each root | [Q19](contract.md#questions-to-answer), which fixes what placement reads |
| More than one frame a query | [Q26](contract.md#questions-to-answer) and [X11](spikes.md#x11-streamed-bodies) |

## Related

[Milestones](milestones.md) for where each row lands; [S18](contract.md) for the questions
some of them wait on; [Known Issues](../appendix/known-issues.md) for items ~~46,~~ ~~92,~~ 93,
~~198~~ and ~~202~~, [Resolved #92, #198](../appendix/resolved/composite-partition-key.md),
[Resolved #46](../appendix/resolved/unmarked-directory-refused.md) and
[Resolved #202](../appendix/resolved/append-batch-bytes.md) for the four that are fixed;
[TODOs](../appendix/todos.md) for the entries these rows were filed under before this part
existed.
