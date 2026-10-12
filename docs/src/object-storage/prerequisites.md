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

**Where it stands (~~2026-10-03~~ ~~2026-10-05~~ ~~2026-10-06~~ ~~2026-10-07~~ ~~2026-10-08~~ ~~2026-10-09~~ ~~2026-10-10~~ ~~2026-10-11~~ 2026-10-12).** ~~Six~~ ~~Seven~~ ~~Eight~~ Nine of the ten required rows are done, each
marked ✅ below. Of the ~~four~~ ~~three~~ ~~two~~ one left, ~~each waits on a
question~~ ~~two~~ ~~three~~ ~~two~~ ~~one can start now, free bytes for every root;~~ the tablet walk waits on a question. [X2](placement-simulation.md) settled what placement reads
([Q19, in part](contract.md#q19-in-part-placement-2026-10-03)), and that frees a member's
failure domain and free bytes for every root. ~~More than one frame a query and the tablet walk
each wait on a question~~ The tablet walk waits on a question ([The order](#the-order)); more than
one frame a query waited on Q26 until X11, below. Of the two dependencies, ~~both wait on their spikes~~ the erasure
coding crate is chosen ✅ by [X4](erasure-coding-crates.md), and the checksum ~~waits on X5~~ is
chosen ✅ by [X5](checksums.md): both are chosen, and neither is in the workspace until its
milestone adds it. [X10](stripe-row-costs.md), which priced the metadata rows these prerequisites
serve, reported on 2026-10-04 ✅ and changes no row of this page: the conditional write it drove was
F68's, on equality alone. [X11](streamed-bodies.md) reported on 2026-10-05 ✅ and answers the part
of Q26 that more than one frame a query waited on, so that row ~~can start~~ started: object bytes travel in
frames of 1 MiB on connections a client keeps apart for long streams. It also found the hop between
executors dear, which makes the optional row for handing a connection over worth building.
[F73](../features/bodies-across-frames.md) ✅ then delivered the row the same day: today's bundles
and answers cross the client wire in bounded frames, over a stream layer M13's object frames will
carry bytes over. [X13](benchmark-shape.md) ✅ reported the same day too and finished what
[F69](../features/driver-operation-kinds.md)'s row left of Q30: a stream makes its own seeded bytes
on one core, faster than any lab device takes them, from a description whose generator is
SplitMix64 in counter mode. It changes no row of this page. [X14](ceph-and-s3-sources.md) ✅ read Ceph
and S3 at the source the same day, with a Ceph on the lab beside the reading, and changes no row
either. It recorded [Q32](contract.md#q32-and-q14-q20-q28-in-part-ceph-and-s3-at-the-source-2026-10-05),
what the metadata leaves room for, which M12's rows take up rather than a prerequisite. The
conditional write it would need is F68's, on the ETag a commit derives.
[X1](stripe-model.md) ✅ modelled the stripe protocol on 2026-10-06 and changes no row of this
page either. The conditional write it needs is F68's as delivered: a stripe's commit compares
the row's sequence and the placement group's generation, both by equality, since everything else
a commit has to respect moves only with the sequence
([the record](contract.md#q16-and-q18-and-q14-q15-q19-in-part-the-stripe-protocol-modelled-2026-10-06)). The prerequisite S16's model was held to, S7's schedules, is now
saved as its acceptance test. The same day the lab's ✅ rotational disks were fitted, one a host,
and [X7](device-store-hdd.md) ✅ measured the device store on them. It changes no row of this page
either. What a rotational device needs is M19's to build
([Q23](contract.md#q23-what-a-rotational-device-needs-2026-10-06)): the disk's write cache
off, its journal on an SSD of the node, an executor never shared with an SSD's slice, and XFS.
[X3](bytes-through-groups.md) ✅ priced candidate A, stripes as rows through today's tablet groups,
on 2026-10-07, and changes no row of this page either: A wrote a byte about twice and reached a
fifth to a quarter of a replicated pool's device, so replicated SSD pools are not tables and the
rows above are needed as they stand
([the record](contract.md#q14-in-part-the-cost-of-stripes-as-rows-2026-10-07)). It found two
defects on the way, neither a prerequisite, since under B no object's bytes are a row: a busy replication lane judged silent
([item 215](../appendix/known-issues.md#215-a-replication-lane-busy-with-wide-rows-is-judged-silent-and-refuses-forwarded-writes)),
and a node past its memory budget while its writes outrun its merges
([item 216](../appendix/known-issues.md#216-writes-faster-than-a-nodes-merges-hold-it-past-its-memory-budget)).
[X8](small-writes.md) ✅ ran one small write three ways on 2026-10-08, and changes no row of this
page either. S7's small-write path is kept, on a device whose sync flushes its cache and below
64 KiB, once a slice shares one flush among its applies; on a device whose cache writes through,
every write is staged ([the record](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)).
What the path needs is M12's and M15's to build: a field of pending bytes in the stripe row, and
a commit that carries them. The byte bound on an append batch it would lean on is ✅ already built,
and a commit of less than 64 KiB is far below [item 208](../appendix/known-issues.md#208-a-write-that-fits-a-client-frame-can-make-a-log-entry-no-peer-frame-carries)'s
frame. It found a client defect on the way, which is not a prerequisite since its fix changes no format
or frame, though an object stream on a connection set apart meets it as any query does: a pooled
connection retired at its lifetime while it still owes answers
([item 217](../appendix/known-issues.md#217-a-pooled-connection-retired-at-its-lifetime-fails-the-answers-it-still-owes)).
[X9](table-latency.md) ✅ measured a table beside object work on 2026-10-09, and changes no row of
this page either: object work runs on executors of its own, a four-core node giving one core up,
and every object loop yields in steps a latency goal can cut
([the record](contract.md#q24-and-q15-in-part-table-latency-beside-object-work-2026-10-09)). That is M14's to build, beside the
slice's owner and the memory budget. To run its encode it brought `rusty_erasure` into the
workspace's lockfile ahead of M18, behind a feature only its harness builds, which the erasure
crate's row below now says. It found a fault in the lab's own procedure on the way, not a
prerequisite: its layout put a benchmark's client on a shard's other thread on the Zen1 hosts
([Resolved #218](../appendix/resolved/lab-core-layout.md)).
[X12](recovery-scrub-rates.md) ✅ measured a rebuild and a deep scrub at four paces beside a
foreground on every lab device the same day, the last spike to report, and changes no row of this
page either. A device's background is paced by its idle time with a byte ceiling, a rotational
pool survives a second loss while it rebuilds, and the parity check runs on a sample of deep scrubs
([the record](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09)): M16's
and M17's to build. It answered Q17 in part, the granularity of a missed write's record, so the
tablet walk still waits on what a driver asks it for. To decode it made `rusty_erasure` a
dependency of `shoal-spike`, an edge the lockfile already held.
✅ **The gate before M11 passed the same day**: P7–P19 agreed, each beside the schedules that
violate it and the tests that own it ([the record](contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)). It changes no row of this page. First
X1's model was extended to X8's small write in its commit, which no model had checked, and it holds
once three local rules are kept ([the small write](stripe-model.md#a-small-write-in-its-commit)); what that path needs is M15's to build,
and the byte bound on an append batch it would lean on is ✅ built.
The first optional row landed the same day: ✅ [F74](../features/client-routing.md) routes a
client's queries by topology, at node level, D7 as the table below has it. It changes no required
row. A client now sends a write to its group's preferred leader, so the hop
[X3](bytes-through-groups.md) saw on two writes in three is gone from a routed client's path, and
on the lab it made writes 1.2 to 1.4 times as fast at bundles of 1 to 64, left reads at `One`
where they were, and, once [item 223](../appendix/resolved/leader-hints-lapse.md) let its leader
hints lapse, took the barrier hop off a strong read ([its lab A/B](../features/client-routing.md#performance)); a client that reads or writes stripe
chunks itself would route by the same table, with a pool map beside it. It fixed
[item 220](../appendix/resolved/stale-topology-retried.md) on the way and item 223 after it, and
filed [221](../appendix/known-issues.md#221-a-strong-exists-takes-no-read-barrier-and-is-never-refused-as-stale-or-steered-off-an-installing-copy),
[222](../appendix/known-issues.md#222-a-second-forward-of-one-bundle-on-a-peer-connection-adds-its-entries-and-not-its-bytes)
and [224](../appendix/known-issues.md#224-a-strong-read-on-a-healthy-cluster-under-load-is-refused-because-the-leaders-heartbeat-round-found-no-quorum),
none a prerequisite.
The second optional row landed on 2026-10-10: ✅ [F75](../features/client-cancel.md) wires `Cancel`
on the client wire, to the depth the user chose, the connection, the shard and the peer. It
changes no required row. A client that stops reading a bundle sends one; the server takes back
every answer it has not written, cuts a streamed answer between two frames, and answers
`Cancelled` instead of running any of the bundle's reads still waiting on a shard of its node or
of a node it forwarded them to - a write still runs, and only its answer is dropped - and a retry
under the same id is answered in full. A reader that
abandons a range of an object will cancel it the same way, and what that saves is the tail of the
range in flight, as the row below always said. On the lab, readers dropping bundles of 64 MiB
after the first answer got through 1.5 times as many a second with cancels on, the members leaving
a quarter of each bundle unwritten, and no cost showed on a node that never cancels
([its lab A/B](../features/client-cancel.md#performance)). It closed the server half of [item 60](../appendix/resolved/stream-connection-accounting.md).
The third optional row landed the same day: ✅ [F76](../features/paged-archive-map.md) pages the
archive map. It changes no required row. A shard held an entry in memory for every row it had ever
archived, about fifty bytes each, which capped a bucket near 27 million objects a GiB of memory a
replica; its index is now a delta of recent changes, immutable runs of 4 KiB pages on disk, and a
cache of those pages, and what it holds in memory is bounded by its settings apart from each run's
filter, about a byte and a quarter a row. A lookup that memory cannot settle is read by the loader,
off the shard's loop, and a conditional insert expecting no row - the object store's path creation
- is answered from the filters without a page read for most keys: in the unit test that holds the
bound, ten thousand lookups of rows never written read 245 pages. On the lab at ten copies of the
TMDB dataset, 23.7 million partitions a node, a node's maps held 62 MiB where the build before held
1.2 GiB, the memory went to rows, and the bench's arms were unchanged
([its lab figures](../features/paged-archive-map.md#performance)).
The fourth optional row landed the same day: ✅ [F77](../features/authorization.md) holds a
principal to what it was granted. It changes no required row. A query on a table its principal
holds no grant for, or holds `read` on and asks to write, is answered `Unauthorized` by the
coordinating shard before it is routed, and the rest of its bundle runs; a principal named nowhere
holds nothing, the cluster's admins hold every table, and a server requiring no authentication
holds nobody to anything. A cluster commits its grants, seeded from the bootstrapper's
`auth.grants` and changed by `grant` and `revoke` admin operations, so every coordinator holds a
principal to the same ones and a revoke reaches connections already open. A grant is held by kind,
and a table is the first: a bucket's grants are a `buckets` map beside it, checked the same way,
which is what "built once for both" asked. On the lab no cost showed: a node nobody authenticates
to was within noise of the build before on every arm, and on the cluster a principal whose every
query is checked ran where that build ran ([its lab A/B](../features/authorization.md#performance)).
The tmdb cluster now names a principal, `reader`, that may read every table and write none.
The fifth optional row landed on 2026-10-11: ✅ [Resolved #93](../appendix/resolved/archived-partition-hash.md)
deleted the archived partition hash, which the user chose over making it agree. It changes no
required row and no key that is persisted. The function hashed a string without its terminator
and every integer but a `u64` or a `u32` as a `u64`, so it was wrong for both of the object
store's keys, `ObjectMeta`'s path and a `u128` object id, which it cut to its lower half. It also
kept a `Uuid` key from compiling. Nothing called it, and nothing can now: a key is hashed from a
row or from a key's values, and a `compile_fail` doctest fails if a third way comes back. The
golden key set froze a `u128` key and a `Uuid` key beside the others, the two types M12 can mint
an object id as, and they are different partitions for the same bits. On the lab, rows written
under string and composite keys before an upgrade to the fix were all found after it, through
every member.
✅ **M11 was delivered the same day**, as [F78](../features/object-harness.md): the first gate,
whose two required rows here, the fixture's faults and the driver's kinds, had landed as F70 and
F69. It changes no row of this page. It built what those rows and X1 had left of the gate: the
byte ledger S16 asks for, which judges every read of an object against the writes under the three
outcomes' contracts and names no shoal crate; the two faults S16 asks for beside F70's, a device
emptied while its node is down and a bit flipped on disk, which were never prerequisites; and the
check that holds this part's acceptance tables to the rules the Distributed chapter's are held to.
On the lab, F69's table capture from before the driver's change and one of today's tree joined
every arm by id, `compare` refusing only each node's memory, which a kernel update had moved by
kilobytes ([item 226](../appendix/known-issues.md#226-compare-refuses-two-captures-whose-nodes-memory-differs-by-kilobytes),
not a prerequisite). Every row [M12](milestones.md#m12-tables-what-the-metadata-needs) waits on
is done.
✅ **M12 was delivered the same day**, as [F79](../features/bucket-schema.md): a bucket is a field
of a schema, `posters: Bucket<Posters>`, which generates its two metadata tables, an identity a
binding and a frame can name, and a place in the grants catalog. It changes no row of this page,
and used three of its required rows as they were built: F68's conditional write is what the
generated rows' `version` and `sequence` are filters for, the composite key of #198 is
`StripeMeta`'s, and #202's bound is what an inline object leans on. It decided what Q25 left to it,
with the user: one stripe table a bucket, and an object id a `u128`
([Q25](contract.md#q25-the-rows-layout-and-the-stripe-table-2026-10-11)). The optional row for
authorization is now built whole, its bucket half included. A client's query of a generated table
is refused at the front door whoever asks, and the generated rows archive to 192 and 360 bytes
against X10's stand-ins' 176 and 312. Every row [M13](milestones.md#m13-the-wire-and-the-baseline)
waits on is done.
The sixth optional row landed the same day: ✅ [Resolved #227](../appendix/resolved/stage-profile-build.md)
made the `stage-profile` build compile again, which it had not since F75. It changes no required
row and no format. That build breaks a query's latency into stages, which is how M13's baseline
and every later gate's evidence will say where an object's time went. F75 had made a released
answer a six-field tuple, and the one loop in each persistent table that only this build compiles,
the fill of a write's durability phases, still named five. Both loops name six now, and the test
that starts a server under the feature holds each table's writes to their phases, so a loop that
compiles and does nothing fails it too. On titan, every insert of both tables' grid arms carried
all four phases, the sync about half of a write's time.
✅ **M13 was delivered the same day**, as [F80](../features/object-wire.md): the first gate that
stores an object. It changes no row of this page, and used every row it waited on as built:
- F73's streams carry an object's bytes, in the same `Data` frames on the same connections set
  apart;
- F68's conditional insert and delete are an object's create, replace and delete;
- #202's bound is under the 16 MiB an inline object may reach;
- F75's cancel covers an object's answer;
- F77's catalog, with F79's bucket half, is what an object operation is checked against.

It added the checksum row's crate, `crc-fast` `=1.10.0`, after reading its `unsafe`. That read found
one defect, inert on the lab, filed upstream's way as a [todo](../appendix/todos.md#guarding-crc-fasts-avx-512-tier).
It found two defects of Shoal's own on the way:
- [item 228](../appendix/resolved/aligned-records.md), a row holding a `u128` behind a pointer,
  which could end its shard at a restart, resolved in the same change;
- [item 229](../appendix/resolved/hotpath-build.md), the `hotpath` build, ~~open and older than
  buckets~~ resolved in a change of its own, below.

~~Neither is a row of this page.~~ Item 228 is not a row of this page; item 229 is the seventh
optional row, below. Every row [M14](milestones.md#m14-devices-and-pools-on-one-node)
waits on is done except ~~the two this page lists for it: a member's failure domain and~~ the one this
page lists for it, every root's free bytes; a member's failure domain is ✅ [F81](../features/failure-domains.md), and every
root's free bytes ✅ [F82](../features/root-capacity.md), both below.
The seventh optional row landed the same day: ✅ [Resolved #229](../appendix/resolved/hotpath-build.md)
made the `hotpath` build of the engine compile again, which it had not since F76. It changes no
required row and no format. That build attributes a run's time to the engine's functions, the
compactor's among them, and is the other half of the attribution M13's baseline and every later
gate's evidence will lean on, beside `stage-profile`. F76 put the writing of an index run under the
archive map's commit, and every chain that commits a map - a compactor's job, a table's start, a
rehome - carried it. Laying out such a chain's future went past the compiler's query depth of 128
once `hotpath` wrapped each method in one more future, and a default build stood four or five
frames short of it. The map's commit is boxed now, in every build, and so is a rehome's fold, the
one place a compactor's work was awaited inline. That took about fifty frames off every chain
through a commit, at one allocation a commit, on no query's path. A test runs a grid arm through
the `hotpath` build and holds its profile to the compactor's scopes, and runbook lines hold both
builds to four futures of room under the limit. On the lab, titan's profiles named the
compactor's whole chain, and the tmdb cluster, upgraded to the fix, read every row back. It found
[item 231](../appendix/resolved/compactor-loop-profiled.md) on the way, the compactor's job loop
profiled as work since a rewrite dropped its skip, and resolved it in the same change; that is not
a row of this page.
✅ **The eighth required row landed on 2026-10-12**, as [F81](../features/failure-domains.md): a
member's failure domain, built as the user decided on 2026-10-03. A node names its host in
`cluster.failure_domains`, the name its machine gives itself when it names none, so nodes on one
machine share one; it is resolved once at start and committed on the record the node observes
itself with, a map of levels so a rack is another key and not a format change. An inventory names
it per node or per group, the node's name by default, and `status` shows each member's host. What
reads it was decided with the user the day before: besides M14's placement, which will read it as
a rule, the rebalance planner and the control group's promotion now prefer a member on a host a
replica set or the voters do not stand on yet, and fall back to what they chose before when none
does, so a cluster on one machine plans and promotes as it did. On the lab, an upgrade from the build
before committed two members with no host, through a leader still on that build, as the rolling
window's limitation says; a `reconfigure` that named every host cleared it, a host moved in the
inventory was committed at the node's next start, and every row read back at quorum. ~~Every row M14
waits on is done except every root's free bytes.~~ Every row M14 waits on is done since F82, below.
✅ **The ninth required row landed the same day**, as [F82](../features/root-capacity.md): every
root's free bytes, built to what the user decided the same day. A node reports each root its
configuration names, the primary first, with its size, its free bytes, the device it is on and what
is written there (the node's own files, the tables' maps and intent logs, their archives). The
leader keeps them in memory and never commits them, and `Members` and `status` show them. The
rebalance planner and the receiver of a snapshot stream now judge a move on every root it writes,
by one rule: every root keeps the disk reserve, and a root a move's bytes land on keeps them too.
So a drain onto a node whose archive disk is full is blocked naming that disk, where it was
planned on the free bytes of the other. The append reserve stays on the WAL's root, as decided.
On the lab, with every node's archives on a disk of their own, each member's two roots were what its
host's `df` and `stat` said, and the archives filled their own disk alone. The cluster was then put
back on one root a node, and every row read back at quorum. It found [item 232](../appendix/resolved/former-leader-capacity.md) on the way,
a former control leader answering capacity it no longer heard, resolved in the same change, and
filed [item 233](../appendix/known-issues.md#233-a-snapshot-install-holds-its-stream-twice-and-the-reserve-check-counts-it-once),
a node with one root needing a stream twice while it installs. Neither is a row of this page.
**Every row [M14](milestones.md#m14-devices-and-pools-on-one-node) waits on is done.**
The eighth optional row landed the same day: ✅ [F83](../features/rack-and-zone-domains.md) puts a
zone and a rack above the host, the two levels the user chose that day. It changes no required row
and no format: F81 made the member's field a map of levels, and a rack and a zone are two more
keys. A node names them beside its host, and an inventory per node or per group, refusing one
machine in two racks or one rack in two zones. A level a member leaves out is the nearest one below
it that it names, so a cluster naming neither plans and promotes as F81's did. On one that names
them, a drain and a voter seat prefer a new zone, then a new rack, then a new host, and a rebalance
lowers none of them. A pool can name `failure_domain: rack` or `zone`, which M14's placement will
read as a rule. On the lab, a rack and a zone named on each group were committed and shown in
`status`, and a rack moved on titan was committed at its next start. An inventory putting titan in
two racks was refused before it touched a host, and every row read back at quorum once the
inventory was put back. It filed [O101](../appendix/optimizations.md#o101-a-quorum-read-of-every-row-runs-at-less-than-half-its-rate-once-every-node-has-restarted)
on the way: a quorum read of every row at less than half its rate once every node had restarted,
on F82's build as on F83's. It also filed [item 234](../appendix/known-issues.md#234-a-plans-move-can-spend-its-stale-version-retries-under-churn-and-a-fencing-test-counts-the-failed-step),
a plan's move that spends its retries under churn and a fencing test that counts the failed step.
Neither is a row of this page.
The ninth optional row landed the same day: ✅ [F84](../features/replication-lane-handoff.md) hands
an accepted connection to another executor, for the replication lane, the one lane the user chose.
It changes no required row and no format. The glommio fork gained `IntoRawFd` for a non-buffered
`TcpStream`, so a connection leaves the executor that accepted it whole, with no `dup`, kTLS
included. A shard's replication link is now one per member, a slot on a peer, and its hello names
the slot in two bytes that were reserved. The shard that accepts it hands the connection to the
executor hosting that slot, so a follower reads every append on the core that applies it: the hop
X11 priced at 1.3 to 1.7 times the cpu a gibibyte is gone from every replicated write's followers.
On europa the replication arms ran 1.04 to 1.06 times as fast; across the lab's 1 GbE an inline
object's put showed no difference within noise, its cost being the row's and not the lane's
([its lab A/B](../features/replication-lane-handoff.md#before-and-after)).
The placement rule puts a tablet's copies on one slot index everywhere, so a shard still opens one
link a peer. Bulk lanes, data lanes and client connections still hop, as the user chose; the bulk
lane is a [todo](../appendix/todos.md#handing-a-bulk-lane-to-its-slots-executor). M15's object lane
will hand its connections the same way. It fixed [item 236](../appendix/resolved/replaced-link-answers.md)
on the way, a replaced link's late answer completing another RPC, and filed [item 235](../appendix/known-issues.md#235-the-client-acceptor-drops-the-tls-session-its-comment-says-it-keeps-for-the-connections-life).
Neither is a row of this page.
✅ **[M14](milestones.md#m14-devices-and-pools-on-one-node) was delivered the same day**, as
[F85](../features/devices-and-pools.md), on the rows above: devices and their slices claimed by
Resolved #46's three outcomes, reported with F82's figures, and placed by F81's and F83's domains,
with a pool whose level no member names placing by F83's fallback, as the user chose. It changes no
row of this page. It moved X9's spike code out of the engine, so the erasure crate's row below no
longer names a feature of shoal-core, and it resolved [item 212](../appendix/resolved/lead-score-logarithm.md),
which no row here named. The one required row left, the tablet walk, is M16's.

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
| ✅ **A failure domain on a member**: delivered by [F81](../features/failure-domains.md) | ~~`MemberRecord` carries addresses, slots, weights and wire facts, and nothing about where the node stands (`shoal-core/src/server/control/types.rs:85`). "A member has no domain and the planner spreads by node alone" ([todos](../appendix/todos.md#distribution), what F46 left undone; [C15](../distributed/open-issues.md#filed-as-unbuilt)). **Where it comes from is decided** (2026-10-03, with the user), though nothing is built: `cluster.failure_domains: {host: <name>}` in a node's configuration, the host defaulting to the OS hostname, so nodes on one machine truthfully share a host and a test that needs several hosts names them; an inventory sets it per node or per group, defaulting to the node's name. Whether placement reads it is Q19's~~ ~~Placement reads it: a pool under a host domain never puts two chunks of a stripe on one host ([X2](placement-simulation.md#recommendation))~~ Since F81, as decided: a node names its host in `cluster.failure_domains`, the OS hostname when it names none, resolved once at start and committed as `MemberRecord::failure_domains`, a map of levels with `host` its one entry (a `rack` and a `zone` beside it since [F83](../features/rack-and-zone-domains.md)), refused by name for a host that cannot be one or a level but `host` (or, since F83, `rack` or `zone`); a restart under another host is committed at its next incarnation. An inventory sets it per node or per group, the node's name by default, and renders it into every file; `Members` and `status` show it. Placement of a stripe's chunks (M14) reads it as a rule ([X2](placement-simulation.md#recommendation)); until then the rebalance planner and voter promotion prefer a new host and never wait for one | A failure domain a node reports and the control group commits, which placement reads | [P11](contract.md#the-contract) counts stripe chunks in distinct failure domains. Without one, two chunks of a stripe can sit on one host, and one host lost is two chunks lost | [M14](milestones.md#m14-devices-and-pools-on-one-node) |
| ✅ **Free bytes reported for every storage root**: delivered by [F82](../features/root-capacity.md) | ~~A node reports one figure: `statvfs` of the default latency path (`shoal-core/src/server/control/capacity.rs:33-55`), carried as `StatusReport::free_bytes` (`shoal-proto/src/shared/protocol/peer/control.rs:229`)~~ Since F82 a node measures every root `Storage::root_uses` lists, the primary first, once a tick: its size, its free bytes, its device (`major:minor`) and its uses (`primary`, `latency`, `throughput`), carried as `StatusReport::roots` (`shoal::shared::capacity::RootCapacity`) with `free_bytes` kept as the tightest root's. The leader keeps them in `NodeCapacity`, never committed, `Members` answers them and `status` lists them. The planner and the snapshot receiver judge a move on every root it writes by one rule (`RootCapacity::need`): the reserve on every root, and the move's bytes too on a root they land on. The append reserve stays on the WAL's root | Free bytes for each root a node writes to, in the report and in the leader's view. Placement draws a device by its size, or by a placement weight the planner fits ([X2](placement-simulation.md#recommendation)), and skips a full one by the reserve check (~~[X2](placement-simulation.md#recommendation)~~ [S4](pools-and-devices.md#capacity-and-weight)), so each device's size and free bytes are what it reads | Placement and the reserve check are judged for each device. With one figure a node, a full disk is found by a write that fails after its commit (~~[S5](placement.md), [S10](recovery.md)~~ [S7's schedule 13](write-path.md#the-schedules-that-shaped-it), [P7](contract.md#the-contract)) | [M14](milestones.md#m14-devices-and-pools-on-one-node) |
| ✅ **More than one frame for one query on the client wire**: delivered by [F73](../features/bodies-across-frames.md) | ~~A request body is read whole into one allocation before anything is routed (`shoal-core/src/server/request_body.rs:55-74`). A frame is bounded at 64 MiB (`shoal-proto/src/shared/protocol.rs:168`), which a client's hello hard-codes (`shoal-client/src/client.rs:558`). A response is one frame a query. `Flags::LAST`, "the last one for its query", is reserved and unused (`protocol.rs:365`)~~ A bundle past the server's frame is an opener and data frames of `MessageType::Data`, assembled before it is routed; an answer past one data frame comes back the same way, with small answers written between its frames; `Flags::LAST` marks a stream's final frame; a client offers its own frame and body bound at the hello and keeps two connections apart for long streams. Streams are granted at the hello by `CLIENT_CAP_STREAMS`, so a client from before F73 is served as it was. A cluster node still assembles no bundle past its frame (item 208) | A write carried in, and a read answered by, a sequence of bounded frames | R13 cannot be met by buffering an object. It is a framing change, and a framing change is cheapest before anything depends on the other shape ([S12](wire-and-client.md)) | [M13](milestones.md#m13-the-wire-and-the-baseline) |
| ✅ **A byte bound on an append batch**: resolved by [Resolved #202](../appendix/resolved/append-batch-bytes.md) | ~~openraft sends up to `max_payload_entries` entries in one append, 300 by default (`openraft-0.10.0-alpha.34/src/config/config.rs:67`), and `group_config` sets no other (`shoal-core/src/server/shard/groups.rs:4430`). The request is framed against the frame bound, and one past it is reported `Unreachable` (`shoal-core/src/server/replication/network.rs:406-422`). Filed as item 202~~ Since Resolved #202 `GroupStore` overrides openraft's `limited_get_log_entries` and cuts every batch at `cluster.replication.append_batch_bytes` (8 MiB by default) of log frames, always one entry; the bound is validated against the frame and the replication queue. One entry larger than a frame is still unsendable ([item 208](../appendix/known-issues.md#208-a-write-that-fits-a-client-frame-can-make-a-log-entry-no-peer-frame-carries)), which an inline object's threshold sits far below | A batch bounded in bytes as well as in entries | An inline object is a row. Three hundred rows of 256 KiB are 75 MiB, so a replica that fell behind a run of small objects could not be fed. Wide rows have the same exposure today | [M12](milestones.md#m12-tables-what-the-metadata-needs) |
| **An engine walk of one tablet's rows that a driver can ask for** | The engine enumerates a tablet's keys for a snapshot and for a scrub (`archived_cut`, `shoal-core/src/server/tables/storage.rs:1010`; `canonical_cut` and `snapshot_partitions`, `shoal-core/src/server/database.rs:373`, `:389`). No query can: a `WHERE` on the partition key is mandatory ([SHQL](../api/shql.md)) | That walk offered to a driver on the leading shard, by tablet and by key range | Backfill of a slice that was away too long, a light scrub, and reclamation of a deleted object's stripes all have to enumerate stripe rows ([S10](recovery.md), [S11](scrub.md)). Backfill is the first to need it | [M16](milestones.md#m16-recovery) |
| ✅ **Torn-write, full-disk and device-loss faults in the fixture**: delivered by [F70](../features/storage-faults.md) | ~~The fixture can exit a process at a named line (`shoal-core/src/server/replication/install.rs:217`), fail a table's intent log write (`shoal-core/src/server/tables/storage.rs:900`) and damage an archive record ([F44](../features/repair.md#what-the-fixture-can-do-now)). [C15](../distributed/open-issues.md#filed-as-unbuilt) files disk-full and torn-archive-write faults as unbuilt~~ Since F70 `shoal::server::faults` arms `Torn`, `Full` or `Lost` for a directory and everything under it, applied through an I/O hook every glommio file asks (the fork's `0308937`, `f4643f7`); the fixture's `FAULT_DIR` arms one in a child, and a root-only test holds the full disk and the lost device to a real device behind device-mapper | A torn write, a full disk and a lost device, for a directory a test names | [P7](contract.md#the-contract) names all three. A clause no test can violate is not checked ([S16](testing.md)) | [M11](milestones.md#m11-step-0-the-harness-and-the-facts) |
| ✅ **Operation kinds beyond read and insert, and byte counters, in `shoal-loadgen`**: delivered by [F69](../features/driver-operation-kinds.md) | ~~Read and insert are written into the mix (`shoal-loadgen/src/spec.rs:21-28`), the operation kind (`window.rs:20-25`), the picker (`pick.rs:22-35`) and the table source (`feed.rs:305`). A window counts operations and no bytes (`window.rs:80-91`)~~ Since F69 a driver is handed `OperationKind<S>`s (`shoal-proto/src/shared/dataset.rs`), from a schema's `DatasetSupport::operation_kinds` or a test, and weighs, picks, times and reports them beside read and insert; every window counts the bytes its streams sent and received on the wire, per kind on the receiving side. A table workload's arm ids, choices and spec digest are frozen unchanged | Kinds a schema's buckets can add, and bytes counted where they are sent and received | R15 asks for it by name, and every gate's evidence is a measurement the driver cannot take today ([S15](performance.md)) | [M11](milestones.md#m11-step-0-the-harness-and-the-facts) |

### Optional

| Prerequisite | What exists today | What it would add | Why it is optional |
| --- | --- | --- | --- |
| ✅ **D7, client routing by topology**: delivered at node level by [F74](../features/client-routing.md) | ~~A client is pushed every topology version and routes by none ([D7](../direction/shard-aware-routing.md))~~ Since F74 a client builds a route table from every pushed frame, by the placement rule the server routes with (now in `shoal-proto`), and sends a write and a strong read to its group's preferred leader and a read at `One` to a holder; a bundle bound for several nodes goes as a run to each. On by default; a stale guess costs the hop it always did. Routing stops at the node: a query is still handed to the executor hosting its slot | A client that picks the node holding what it wants | Only a client that writes or reads stripe chunks itself needs it. A node coordinates in every design here, so nothing built changes when D7 arrives, and D7's own rule is to measure the hop first ([Q15](contract.md#questions-to-answer)), which F74's lab A/B did: writes 1.2 to 1.4 times as fast, reads at `One` unchanged |
| ✅ **`Cancel` on the client wire**: delivered by [F75](../features/client-cancel.md) | ~~Reserved as message type 12 and unwired (`shoal-proto/src/shared/protocol.rs:216`; [todos](../appendix/todos.md#cancel-and-what-it-would-actually-buy))~~ Since F75 a client granted `CLIENT_CAP_CANCEL` sends `Cancel` (a header and the bundle's id) for a bundle it stops reading, answered by one `Error` frame of code `Cancelled`; it covers the arrivals of the bundle on its connection before it, by the coordinating shard's attempt bound. The relay takes back what it owes of them and cuts a streamed answer between frames, every shard reads the node's `CancelBoard` before it runs a query and answers a covered read `Cancelled` (a write still runs), and a `PeerCancel` follows the forwards to a peer that negotiated `CAP_CANCEL_V1`. Work a shard has begun still runs | A reader abandoning a range it no longer wants | With bounded ranged frames a reader stops by not asking for the next range; what a cancel saves is the tail of one. ~~The type is reserved, so wiring it later changes nothing already on the wire~~ Wired by F75 before any object frame exists, so M13's ranged reads cancel through it as tables do |
| ~~**A clone call in the glommio fork**~~ **Not needed**: [X6](device-store-ssd.md#3-a-partial-write) rejected the clone | `copy_file_range_aligned` exists (`glommio/src/io/dma_file.rs:590` at `f4643f7`, `:574` before F70); no `FICLONERANGE` | Splicing a staged range into a stripe chunk without copying it | ~~Needed only if [X6](spikes.md#x6-the-device-store-on-ssd) picks that way of applying an update~~ X6 did not: a clone's sync cost three to eight times an overwrite's and a cloned chunk read cold at 2.4 to 2.6 times a fresh one's. The journal and the apply in place stay ([Q22, in part](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04)) |
| ✅ **Handing an accepted connection to another executor**: delivered for the replication lane by [F84](../features/replication-lane-handoff.md) | ~~Every shard accepts on the shared port, and a frame naming a slot is handed to the executor hosting it (`shoal-core/src/server/peer/listener.rs:181`). Nothing moves a connection. The fork's `TcpStream` has `FromRawFd` and no `IntoRawFd`; X11 handed one over with a `dup`~~ Since F84 the fork's non-buffered `TcpStream` has `IntoRawFd` (`d781abd`), which gives the descriptor up with no `dup`. A replication link is one per member and its hello names the slot; the shard that accepts it gives the connection, whole and kTLS included, to the executor hosting that slot (`peer::handoff`). Every other lane is served where it landed and hands each frame to the shard it names, as before | Bytes read by the executor that owns the slice they are for | ~~Needed only if [X11](spikes.md#x11-streamed-bodies) finds the hop between executors too dear~~ X11 did: bytes hopping as buffers cost 1.3 to 1.7 times the cpu a gibibyte at 1 MiB in plaintext, while a connection under kTLS was handed over at no cost ([X11](streamed-bodies.md#4-a-connection-handed-over-and-bytes-that-hop)). Still optional, since adding it changes no format; ~~worth building for M14's object lane~~ built for the replication lane, whose followers read every replicated write, and the mechanism M15's object lane will hand its connections with |
| ✅ **A failure domain above the host**: delivered by [F83](../features/rack-and-zone-domains.md), as a zone and a rack | ~~None~~ Since F83 a node names a `rack` and a `zone` above its host in `cluster.failure_domains`, and an inventory per node or per group, refusing a machine in two racks or a rack in two zones. Both are committed on `MemberRecord::failure_domains` beside the host and shown in `status`. The drain, the rebalance and voter promotion read them widest first, a level left out being the nearest one below it (`shoal::shared::domains::domain_at`). A pool's `failure_domain` takes `rack` and `zone` beside `device` and `host` | Stripe chunks spread over racks | ~~No deployment has a rack to name. The member's field is a list from the start, so a level is added without a format change~~ No deployment had a rack to name, and nothing built had to change for one: F81's field was a map of levels, and the two went in as two keys, no format moved. M14's placement reads a pool's `rack` or `zone` as it reads `host` |
| ✅ **Paging the archive map**: delivered by [F76](../features/paged-archive-map.md) | ~~The map holds an entry for every row in memory, about fifty bytes each, and nothing evicts it ([todos](../appendix/todos.md#a-nodes-archive-map-is-bounded-by-nothing))~~ Since F76 a map's index is paged: a delta of recent changes backed by its intent log, immutable runs of 4 KiB pages in key order with each run's directory and Bloom filter in memory, and a cache of pages, under a manifest that is its commit point. What it holds in memory is bounded by its settings apart from the filters, about a byte and a quarter a row at ten bits a key | A bucket larger than memory allows | ~~It is a ceiling, near twenty million objects a GiB of memory a replica, and not a correctness matter. It lifts inside the table engine without touching an object format.~~ It was a ceiling and not a correctness matter, and it lifted inside the table engine without touching an object format: the filters leave room for about 860 million rows a GiB a replica, and `filter_bits: 0` leaves no ceiling at all, at a page read for every lookup of a row that is not there. [S3](objects.md#what-it-costs) states what is left |
| ✅ **Authorization**: delivered for tables by [F77](../features/authorization.md), built once for buckets too | ~~Tables have none: an authenticated principal can read and write any table ([todos](../appendix/todos.md#per-table-authorization))~~ Since F77 a principal is held to a catalog of grants, by principal and then by table or `*`, each `read`, `write` or both: the coordinating shard answers a query on a table its principal holds too little on `Unauthorized` before anything is routed, a principal named nowhere holds nothing and `cluster.admins` hold everything. A standalone server reads `auth.grants`; a cluster seeds its committed policy from the bootstrapper's and changes it with the `Grant` and `Revoke` admin kinds, carried to every shard in the map. A principal's grants are held by kind, a table the first | A principal held to some buckets | A bucket is no less protected than a table is. It should be built once, for both. ~~Built once~~ Built: a bucket's grants are a `buckets` map beside `tables`, under the same privileges and checked by the same coordinator, which [M12](milestones.md#m12-tables-what-the-metadata-needs) ~~adds~~ added with the bucket ([F79](../features/bucket-schema.md)), and its generated tables are reached through the bucket's grant alone ([S2](buckets.md#what-the-macro-generates)): no table grant reaches one, `*` included, and the front door refuses a client's query of one whoever asks |
| ✅ **Known issue 93: the archived hash of a string key**: resolved by [Resolved #93](../appendix/resolved/archived-partition-hash.md) | ~~`get_partition_key_from_archived_insert` hashes a string without the terminator the live path writes, so the two disagree for every string key. Nothing calls it ([item 93](../appendix/resolved/archived-partition-hash.md))~~ Since Resolved #93 there is no archived hash: `PartitionKeySupport` hashes a row or a key's values and nothing else, and a `compile_fail` doctest on `shoal::traits` fails if the method returns. It was wrong for more than a string: every integer but a `u64` or a `u32` was written as a `u64`, so a `u128` object id lost its upper half, and a `Uuid` key did not compile. One compiles now, and four `u128` keys and three `Uuid` keys joined the golden key set | The function deleted, or made to agree and frozen beside the live hash | `ObjectMeta` is keyed by a path, a string, so it is the table this would hit. But the function has no caller and this design adds none, and either fix changes no key that is persisted, so it can be done at any time. It is listed so that nobody gives it a caller first. Deleted, so nothing can be given it as a caller: a caller would have to write a hash of its own, and meet the doctest on the way. On the lab, rows written before the upgrade to it were all found after |
| ✅ **Known issue 227: the `stage-profile` build of the engine**: resolved by [Resolved #227](../appendix/resolved/stage-profile-build.md) | ~~`shoal-core` built with `stage-profile` fails with two `E0308`s: each persistent table's `get_flushed` fills a released write's durability phases in a loop only that build compiles, and it names five fields of the six-field tuple F75 made of a released answer. No test would notice the loop doing nothing ([item 227](../appendix/resolved/stage-profile-build.md))~~ Since Resolved #227 both loops name six fields and the build compiles. `stage_join.rs` runs both persistent tables' grid arms under the feature and holds their writes to `durable_write` and `durable_sync`, so a loop that compiles and does nothing fails it too | A query's latency broken into stages, the durability phases among them, for M13's baseline and every later gate's evidence ([S15](performance.md)) | It is an attribution build that no shipping node runs, and fixing it changes no key, file or frame, so it could land at any time. It was listed so that M13's baseline did not start without a working stage breakdown. On titan, every insert of both tables' grid arms carried all four phases |
| ✅ **Known issue 229: the `hotpath` build of the engine**: resolved by [Resolved #229](../appendix/resolved/hotpath-build.md) | ~~`shoal-core` built with `hotpath` fails with `queries overflow the depth limit!` laying out the compactor's `fold` future, and raising the limit in `shoal-core` moves the error into the schema crate's generated `new`. Nothing builds the feature but the runbook line ([item 229](../appendix/resolved/hotpath-build.md))~~ Since Resolved #229 the archive map's commit and its rehome stage reach the paged index through a boxed future, and a rehome awaits a compactor's fold boxed, so the index's run writes and a compactor's jobs are laid out on their own and not inside every caller's future; both builds of `shoal-core` and the bench schema's crate compile with the limit lowered to 112. `hotpath_profile.rs` runs a grid arm through `shoal-workload` built with the feature and holds its profile to the compactor's scopes | A run's time attributed to the engine's functions, the compactor's among them, for M13's baseline and every later gate's evidence ([S15](performance.md)), beside `stage-profile`'s stages | It is an attribution build that no shipping node runs, and fixing it changes no key, file or frame, so it could land at any time. It was listed so that no gate's evidence leans on a profile that cannot be built. The default build was within one nested future of the same failure, and gained the same room |

## Dependencies to choose

Not gaps in the code, but nothing can be built without them, so both are **required**. Neither
is chosen on this page: each is chosen by its spike and recorded on
[S18](contract.md#decision-record).

| Dependency | What exists today | Chosen by |
| --- | --- | --- |
| ✅ **An erasure coding crate** (R11): `rusty_erasure` 0.4.1, Reed-Solomon over ISA-L's Cauchy matrix, chosen by [X4](erasure-coding-crates.md) | ~~None. No erasure coding crate is in `Cargo.lock`~~ ~~Still none in the workspace's `Cargo.lock`, deliberately: [M18](milestones.md#m18-erasure-coding) adds it.~~ In the workspace's `Cargo.lock` since [X9](table-latency.md), ahead of M18 and decided with the user, ~~behind shoal-core's `x9` feature, which only X9's harness builds~~ through `shoal-spike` alone since [F85](../features/devices-and-pools.md) removed the `x9` feature: no default build links it, and [M18](milestones.md#m18-erasure-coding) makes it a dependency of the store. X4 measured seven candidates in a harness outside the workspace (`shoal-spike-erasure/`), and the one chosen writes parity byte for byte the same as ISA-L's C library at every layout it was run at, so the format it fixes on a device is ISA-L's and not one crate's | ~~[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs), which compares code families and crates, before [M18](milestones.md#m18-erasure-coding). The candidates are pinned on [S18](contract.md#decision-record)~~ Chosen by X4 on 2026-10-03 and recorded on [S18](contract.md#q20-in-part-the-code-and-the-crate-2026-10-03), with what it did not settle |
| ✅ **A checksum with a frozen definition** (R18): CRC-64/NVME through `crc-fast` 1.10.0, with a combine of Shoal's own, chosen by [X5](checksums.md) | ~~gxhash 2.3 is the one hash in the workspace and is already pinned "as a persistence format" (`Cargo.toml:29-34`), a pin that exists because two majors once disagreed ([Resolved #65](../appendix/resolved/gxhash-pin.md)). `crc32fast` is in the lockfile through other crates' dependencies and nothing in Shoal calls it~~ ~~Still gxhash alone in the workspace for the tables, and no CRC called by Shoal: [M13](milestones.md#m13-the-wire-and-the-baseline) adds `crc-fast` (the lockfile has held it since [X13](benchmark-shape.md), whose spike checksums with it, and no Shoal crate calls it).~~ Still gxhash for the tables. Since [F80](../features/object-wire.md) (M13), `shoal::shared::checksum` is CRC-64/NVME through `crc-fast` `=1.10.0` with default features off, and an object's put and read name one in their heads. X5 measured eight crates in a harness outside the workspace (`shoal-spike-checksum/`). gxhash gave the same output on every cpu and build, but not however it was fed: its `Hasher` cut into pieces never equals its one-shot function. CRC-64/NVME is a definition of six parameters and a check value, which two crates met on every host and build; it combines and resumes, so a client's checksum of a unit can be the one a slice stores | ~~[X5](spikes.md#x5-checksums), before [M13](milestones.md#m13-the-wire-and-the-baseline), since a frame that carries a unit's checksum fixes it on the wire. Keeping gxhash is a possible answer; so is a CRC, whose definition no crate's release can move~~ Chosen by X5 on 2026-10-03, before M13 as required, and recorded on [S18](contract.md#q21-in-part-the-checksum-2026-10-03), with what it did not settle: the granule, and whether the row keeps a chunk digest |

## What the lab needs fitted

The lab is europa, titan and hyperion, the hosts of `tmdb_cluster.yaml`
([cluster testing](../cluster-testing/overview.md#the-lab)).

| Item | Needed | Why |
| --- | --- | --- |
| ✅ **Rotational disks**: fitted 2026-10-06, one in each host at `/dev/sda`: a WD140EDFZ (14 TB, CMR; it reports 5400 rpm and turns at 7200) in titan and in hyperion, a WD6001FZWX (6 TB, 7200 rpm, CMR) in europa. Each is one partition, left by [X7](device-store-hdd.md) as an empty XFS at `/hdd`, and by ✅ [X12](recovery-scrub-rates.md) as it found it | Yes, for [X7](spikes.md#x7-the-device-store-on-hdd), [X12](spikes.md#x12-recovery-and-scrub-rates) and [M19](milestones.md#m19-rotational-devices). One in a Zen1 host is the least that answers X7; two in each host let a 4+2 layout run over real devices. ~~Not fitted~~ One in each host: X7 is answered, and a 4+2 over real disks still waits on a second disk a host | R16 cannot be judged on an emulated disk. A fixed delay has no seek in it, so it ranks an append to a journal and a random write in place the same, and that ranking is what the spike is for |
| ✅ **An XFS filesystem**: fitted 2026-10-03, europa's Optane at `/optane`, and an LV on titan's and hyperion's 970 EVO at `/xfs` | Yes, for [X6](spikes.md#x6-the-device-store-on-ssd) | ~~The lab's devices are ext4 on titan and hyperion and btrfs on europa.~~ europa's Optane is XFS, its root btrfs; titan and hyperion are ext4 at the root, each with an XFS volume beside it on the same device, which is what lets [X6](device-store-ssd.md) compare filesystems on one device. The book's own guidance is XFS, and btrfs is recorded as a poor host for this write path ([Storage Overview](../storage/overview.md#limitations)) |
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
| ~~Items 198, 46 and 202; the fixture's faults; the driver's kinds and byte counters. None depends on an open question, and each is worth having with no object store at all~~ All done | — |
| ✅ Item 198, with item 92, resolved by [Resolved #92, #198](../appendix/resolved/composite-partition-key.md) | Done |
| ✅ Item 46, resolved by [Resolved #46](../appendix/resolved/unmarked-directory-refused.md) | Done |
| ✅ Item 202, resolved by [Resolved #202](../appendix/resolved/append-batch-bytes.md) | Done |
| ✅ The fixture's storage faults, delivered by [F70](../features/storage-faults.md) | Done. An emptied device and a flipped bit, which S16 also asks for, are not prerequisites and ~~are not built~~ were built with M11 by [F78](../features/object-harness.md) |
| ✅ The driver's kinds and byte counters, delivered by [F69](../features/driver-operation-kinds.md) | Done. [Q30](contract.md#questions-to-answer) is recorded ~~in part~~ whole: the one driver is generalized; ~~the object dataset and the seeded-bytes rate are left to [X13](spikes.md#x13-the-benchmarks-shape)~~ the object dataset and the rate of seeded bytes were ✅ [X13](benchmark-shape.md)'s ([Q30](contract.md#q30-the-object-dataset-and-seeded-bytes-2026-10-05)) |
| ✅ The conditional write, delivered by [F68](../features/conditional-writes.md) | ~~Nothing, for a condition on one field.~~ Done, for equality on any of a row's filter fields. [Q25](contract.md#questions-to-answer) settles whether the generated rows need more, such as a comparison other than equality. [X10](stripe-row-costs.md) drove both rows' commits on equality of one field each, and [Q25, in part](contract.md#q25-in-part-the-metadata-rows-2026-10-04) asks for no more |
| ✅ The contract agreed, every decision on S18's record: the gate before [M11](milestones.md#before-m11-the-object-contract) | Done 2026-10-09, once X1's model had checked the small write in its commit ([the record](contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)). Not a row of this page's tables, since it is a decision and not a gap in Shoal; listed so the order reads whole |
| The tablet walk | [Q17](contract.md#questions-to-answer), which says what a driver asks it for |
| ✅ A member's failure domain, delivered by [F81](../features/failure-domains.md) | Done |
| ✅ ~~The failure domain and~~ Free bytes for each root, delivered by [F82](../features/root-capacity.md) | Done. ~~[Q19](contract.md#questions-to-answer), which fixes what placement reads~~ ~~Nothing. [X2](placement-simulation.md) fixed what placement reads ([Q19, in part](contract.md#q19-in-part-placement-2026-10-03)), so it could start today, landing no later than [M14](milestones.md#m14-devices-and-pools-on-one-node)~~ |
| ✅ More than one frame a query, delivered by [F73](../features/bodies-across-frames.md) | ~~[Q26](contract.md#questions-to-answer) and [X11](spikes.md#x11-streamed-bodies)~~ ~~Nothing. X11 answered the part of Q26 it needed ([Q26, in part](contract.md#q26-in-part-streamed-bodies-2026-10-05)): frames of 1 MiB, on connections set apart for long streams~~ Done, to what X11 chose: frames of 1 MiB, on connections set apart for long streams |

None of these stands before a spike. What the spikes themselves need first, checked spike by
spike, is on [the spikes page](spikes.md#what-a-spike-needs-first), and the whole order of the
work, this page's rows among it, is drawn on [What's left to do](whats-left-todo.md).

## Related

[Milestones](milestones.md) for where each row lands; [S18](contract.md) for the questions
some of them wait on; [Known Issues](../appendix/known-issues.md) for items ~~46,~~ ~~92,~~ ~~93,~~
~~198,~~ ~~202,~~ ~~227~~ and ~~229~~, [Resolved #92, #198](../appendix/resolved/composite-partition-key.md),
[Resolved #46](../appendix/resolved/unmarked-directory-refused.md),
[Resolved #202](../appendix/resolved/append-batch-bytes.md),
[Resolved #93](../appendix/resolved/archived-partition-hash.md),
[Resolved #227](../appendix/resolved/stage-profile-build.md) and
[Resolved #229](../appendix/resolved/hotpath-build.md) for the ~~four~~ ~~five~~ ~~six~~ seven that are fixed;
[TODOs](../appendix/todos.md) for the entries these rows were filed under before this part
existed; [X4's record](erasure-coding-crates.md) for the erasure coding crate; [X5's
record](checksums.md) for the checksum; [X2's record](placement-simulation.md) for what placement
reads; [X11's record](streamed-bodies.md) for the frames, and [F73](../features/bodies-across-frames.md)
for the row they set; [F81](../features/failure-domains.md) for a member's failure domain;
[F82](../features/root-capacity.md) for every root's free bytes; [F83](../features/rack-and-zone-domains.md)
for a failure domain above the host; [F84](../features/replication-lane-handoff.md) for handing a
connection to another executor.
