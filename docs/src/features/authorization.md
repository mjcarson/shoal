# F77. Authorization

A principal is held to what it was granted. A connection that authenticated
([F12](authentication.md)) carries a principal, and since this feature every query it sends is
judged against a catalog of **grants**: by principal, then by table, each naming `read`, `write`
or both. A query on a table its principal holds too little on is answered `Unauthorized` by name -
who, what, which table, and what it does hold - and is never routed, so it reads and changes
nothing. The rest of its bundle runs. A principal the catalog does not name holds nothing; a
cluster's admins hold every table; and a server that requires no authentication has no principal
to hold, so it serves every client as it always did.

A standalone server reads its grants from `auth.grants`. A cluster seeds them once, from the
bootstrapping node's `auth.grants`, into its committed policy, and changes them after with the
`grant` and `revoke` admin operations, so every coordinator holds a principal to the same grants
and a change reaches connections already open.

## Context

This is the optional S1 prerequisite "Authorization"
([S1](../object-storage/prerequisites.md#optional)): a bucket is no less protected than a table
is, so authorization should be built once, for both. It had sat in
[TODOs](../appendix/todos.md#per-table-authorization) since D3, filed as blocked on a principal
and then, once F12 built one, as unblocked and undone. The todo named the three parts: somewhere
to store grants, a check on the query path, and a way to express them.

What was there:

- **A principal on every authenticated connection**, logged and carried by the client relay for
  its admin requests, and consulted by nothing else (`Principal`, `shoal-proto/src/shared/auth.rs`).
- **Committed cluster policy, pushed to every shard.** `cluster.admins` is seeded into the control
  state at bootstrap and carried in every tablet map ([F39](membership.md)); a table's read policy
  is committed by an admin operation and carried the same way ([F41](read-consistency.md)). An
  admin mutation is refused `Unauthorized` unless `cluster.admins` names its principal.
- **One coordinating shard per connection.** The shard that accepted a connection routes every
  bundle it sends; a share forwarded to another node arrives on a peer lane, which has no
  principal ([F38](inter-node-transport.md)).
- **A deployed cluster with one user.** `shoaladm` renders `auth.required` and the admin's derived
  credential into every node's file, and every tool it builds connects as that admin
  ([F51](cluster-deployment.md)).

Decided with the user on 2026-10-10: grants committed and seeded from the file, every principal
denied what no grant names with admins holding everything, privileges of read and write, and admin
commands as the way to write them, SHQL staying read-only.

## What it does

### The catalog

`shoal-proto/src/shared/grants.rs`:

```yaml
auth:
  required: true
  users:
    reader: {password: hunter2}
    loader: {password: hunter3}
  grants:
    reader:
      tables:
        Movie: [read]
        Review: read, write
    loader:
      tables:
        '*': [read, write]
```

- **`Privileges`** is two bits, `READ` and `WRITE`, written as a list of names or one string of
  them; `all` reads as both. A get, an exists and a filter need `read`; an insert, an update and a
  delete, conditional or not, need `write`. Neither implies the other.
- **`PrincipalGrants`** holds a principal's grants by kind, and a table is the only kind:
  `tables`, keyed by the name the schema spells the table, or `*` for every table. What a principal
  holds on a table is its grant on that name and its grant on `*` together. Unknown keys are
  refused, so a `buckets` map from a later build is never read as a principal holding nothing.
- **`Grants`** is the catalog: principals by name, in ordered maps so every node serializes it the
  same way. `grant` adds to what is held, `revoke` takes back what a grant on the same name gave,
  each says whether anything changed, and a principal left holding nothing leaves the catalog.

A grant on a table the schema does not serve is refused where it is made: at a standalone node's
start (`ShoalPool::start`, through `Auth::check_grants`), at a proposal (the control thread
resolves the name against its schema), and at `shoaladm`'s deploy (against the schema of the
program it runs as). A grant on nothing never exists.

### Committed on a cluster

`BootstrapPolicy` carries `grants`, which `ControlPlane::start` takes from the node's
`auth.grants`; only a fresh bootstrap commits it, so a joiner's own copy is ignored as its
`cluster.admins` is. After it, three admin kinds:

| Kind | Does |
| --- | --- |
| `Grant { principal, table, privileges }` | Adds privileges on a table, or `*`, to a principal's grants. A mutation: an admin, a version and an operation id, as every mutation |
| `Revoke { principal, table, privileges }` | Takes back privileges a grant on that name gave. Takes nothing from a grant on `*` |
| `Grants` | Reads the catalog: whole to an admin, and on a cluster that requires no authentication; any other principal is answered its own grants alone. Advertised in `admin_reads` |

Each is a `ControlCommand` applied in committed order (`ControlState::change_grants`): refused
before a bootstrap, for an empty principal or privilege set, for a stale version, and - once an
initialization recorded the tables - for a table the placement was not initialized with. A grant
of what is held and a revoke of what is not are applied without moving the version, as a read
policy set to what it already is. A change moves the version, so the next map carries it to every
shard; the client's topology frame never does.

### Checked by the coordinator

Every shard compiles the catalog into an `Authorizer` (`shoal-core/src/server/authorize.rs`): each
principal's privilege on each of the schema's tables, by the table's position among
`QuerySupport::table_names` (`TableNameSupport::position`, generated), and `Access::Everything`
for each admin. A standalone shard compiles its file's grants once; a cluster shard compiles the
map's grants and admins at its start and again with every map it installs.

The client relay's `NewClient` broadcast carries the connection's principal, so the coordinating
shard knows it before the connection's first bundle. In `send_to_shard`, before admission and
routing, `restricted_access` answers nothing for a shard no authenticated client is on, a
connection with no principal and an admin; for anyone else `authorize` reads each query's table
and whether it writes off the archive, refuses each one its principal holds too little on, and
logs the refusal once a bundle at `INFO` with the principal and the count. A refused query is
answered under its own index:

```text
reader may not write Movie: it holds read on it
stranger may not read Secret: it holds nothing on it
```

The check is per bundle against the grants in force, never cached on the connection, so a revoke
reaches a connection already open at its next bundle. A share forwarded to a peer is not checked
again: the peer trusts the coordinator that forwarded it, as it trusts its routing.

### On a deployed cluster

An inventory names `users` beside its `admin`, and `grants`
([shoaladm](shoaladm.md)):

```yaml
admin: admin
users: [reader]
grants:
  reader:
    tables:
      '*': [read]
```

Each user's password is minted the first time a deployment needs it and kept beside the admin's
in the deployment's state, `user-<name>.password`; every node's file carries each user's derived
credential and the grants, which the bootstrap seeds. `SHOAL_DEPLOY_USER=<user>` makes every
connection a command opens for itself - `admin`, `status`, `stats`, the loader, the bench driver,
`shoalctl` - authenticate as that user instead of the admin. The deployment's own steps stay the
admin's whoever the command runs as (`connect_as_admin`, `any_member_as_admin`): a bootstrap's
initialization, an add, an upgrade's checks and activation, a reconfigure, a rebuild, and a
bench's restarts and events, so a run as a user cannot stop a deployment part way. A deploy
refuses an inventory granting on a table its schema does not serve. The cluster tab's command
line and `shoaladm admin` take:

```text
grant <principal> <table|*> <read|write|read,write>
revoke <principal> <table|*> <read|write|read,write>
grants
```

## Design choices

**Grants are committed policy, seeded from the file**, the way `cluster.admins` is. Every
coordinator then holds a principal to the same grants, a change is versioned, idempotent by its
operation and audited like every other mutation, and it reaches every node without a restart. The
file is still where a grant is written first, because a standalone node has no control group and
a cluster needs grants before its first admin connects.

**The coordinating shard checks, once a bundle, before anything is routed.** It is the one place
every client query passes with its principal known and nothing yet done. A check at each executing
shard would cost every share of a split query and need the principal on every forward; a peer lane
carries none, and the coordinator already chose where every share goes.

**Nothing is allowed that no grant names, and an admin holds everything.** A principal is denied
by default because the alternative - open until the first grant exists - leaves a server open
whenever an operator forgets the grants, and silently closes it to everyone the moment one is
added. An admin is not held to the catalog because it can grant itself anything: refusing it a
table would protect nothing and break every tool that connects as the deployment's admin.

**Read and write, nothing finer.** They are what a bucket needs - get and head, put and delete -
and what a table's queries split into by `archived_is_write`, which the coordinator already reads.
A finer privilege is a new name later, since privileges are written by name, so nothing has to be
undone to add one.

**The catalog is keyed by names, not table identities.** A grant is written by an operator, `*`
has no `TableId`, and a `TableId` is the hash of the name anyway. Names are checked where a grant
is made, and compiled to positions where a query is checked.

**Compiled to a privilege a table by position.** A table's position indexes the schema's own list,
so a query's check is an index and a mask, after one lookup of its principal a bundle. Positions
move when a table is added ahead of another, which is why they index only what a node compiled
for itself and never reach a peer, a file or the wire.

**Checked against the grants in force on every bundle**, rather than resolved once per
connection. A revoke has to reach the pooled connections a client already holds, and a lookup a
bundle is cheaper than a registry of connections to invalidate.

**`Unauthorized` for both halves.** The code an admin refusal already carried names what a client
does with either: stop, and ask an admin. A refused query was never routed, so a retry is refused
the same way and nothing about the table changed.

**Built once, for a bucket too.** A grant is held by kind, a table the first; a bucket will be a
`buckets` map beside `tables` with the same privileges, checked by the same coordinator. Its
generated metadata tables are refused at the front door by name whoever asks
([S2](../object-storage/buckets.md#what-the-macro-generates)), so a bucket's grant is the only
grant that reaches them.

## Alternatives rejected

**Grants in each node's file alone.** Read at startup like `auth.users`, so a change is a restart
of every node, and two nodes whose files differ answer one principal two ways depending on which
it reached - which [F74](client-routing.md)'s routing makes a matter of key, not choice.

**Open until a grant exists.** An opt-in that costs nothing to adopt, and the wrong default for the
reason above: authorization that switches on when the first grant is added is authorization an
operator turns on for everybody by granting something to somebody.

**A privilege for each query kind** - get, exists, insert, update, delete. Offered, and not taken:
a bucket's operations do not map onto a table's kinds, so the catalog would need a second
vocabulary for buckets, and every refinement can be added as a name later.

**Admins held to the catalog like anyone else.** Separation of duties a catalog cannot enforce,
since the admin writes the catalog.

**A check at each executing shard.** Described above: more work, and a principal on the peer
wire, for a decision the coordinator has already made.

**`GRANT` and `REVOKE` in SHQL.** SHQL is a read parser a human types into the query bar
([SHQL](../api/shql.md)); grants are admin operations, versioned and audited, which the cluster
tab's command line already takes beside the others. Filed in
[TODOs](../appendix/todos.md#grant-and-revoke-in-shql).

**Roles, or grants to groups of principals.** Not needed by anything here, and a catalog keyed by
principal leaves room for a `roles` kind beside `tables`.

## Limitations

- **A standalone server's grants are read once, at its start**, like its users; it has no control
  group to commit a change through, and refuses every admin operation.
- **A change reaches each node when it installs the map that carries it.** For the moment between
  two nodes installing it, a principal is answered by the old grants through one and the new
  through the other, and a bundle already authorized runs to its end.
- **The inventory's grants seed a bootstrap and nothing after it.** Editing them on a cluster that
  exists changes no grant - `admin grant` and `admin revoke` do - though it re-renders every node's
  file, so a `reconfigure` restarts the nodes for nothing. Filed in
  [TODOs](../appendix/todos.md#the-inventorys-grants-on-a-cluster-that-exists).
- **The wizard has no fields for users or grants.** It carries an inventory's through unchanged.
- **Admin reads are open to every authenticated principal.** `Members`, `Readiness`, `Stats` and
  the rest need no grant; `Stats { table }` reports a table's rows and bytes, though never a row.
- **A writer that may not read learns one bit a conditional write**: whether its condition held,
  which is what the condition is for ([F68](conditional-writes.md)).
- **A grant may name a principal no node accepts yet**, and is held for it until one does. Adding
  a user is still a restart ([F12](authentication.md#limitations)), which `shoaladm reconfigure`
  does.
- **No roles, no grants on columns or rows, no SHQL.** Each is filed above or needs nothing yet.
- **The bucket half is a kind of grant, not a built one.** Buckets do not exist; the catalog leaves
  their map's place and the coordinator its check.

## Invariants to uphold

**Every client query passes `authorize` before it is routed.** It sits in `send_to_shard`, which
both a bundle read whole and one assembled from a stream reach. A new path by which a client's
query reaches a shard has to pass it too, or a principal reaches what it was refused.

**A connection's principal reaches its coordinator before its first bundle.** It rides
`NewClient`, which the acceptor broadcasts before the read relay starts, on the channel the
bundles follow.

**A principal named nowhere compiles to nothing on every table**, never to a missing entry the
check could read as unrestricted; a position past the schema's tables reads as nothing too.

**A connection with no principal is never held to the grants, and exists only where nothing is
required.** The check's fast path is `principals.is_empty()`, one length load a bundle, which is
what keeps every benchmark that does not authenticate where it was.

**`TableNameSupport::position` is the table's index in `QuerySupport::table_names`.** The derive
emits both from the same fields in the same order, and
`authorization::a_tables_position_is_its_place_among_the_table_names` holds them together.

**Grants change only through committed commands, applied purely.** `change_grants` moves the
version only on a change, so a repeat pushes no map; the map carries the grants to shards and the
client's topology frame never does.

**Privileges are written by name, and a name is never reused.** `read` and `write` are on disk in
the control state and in every config file; a new privilege is a new name.

**A refusal is `Unauthorized` and the query was not routed.** A client that retries is refused the
same way, and a refused write changed nothing.

## Performance

**On a server that requires no authentication, one length load a bundle**: no connection has a
principal, `principals` is empty, and `restricted_access` returns before any lookup. That is every
benchmark and capture in the corpus. **An admin's bundle costs two hash lookups**, its connection's
principal and the principal's access, and no per-query work. **A principal held to grants adds,
for each query, reading its table and whether it writes off the archive, a match to its position
and a mask**, and nothing is allocated unless something is refused.

Measured on the lab, two ways. **These are A/Bs, not captures**; a difference counts only when the
sides' run intervals are disjoint.

### On one node, nobody authenticated

**On titan** (Ryzen Embedded V1756B, Zen1, 4 cores and 8 threads, kernel 7.0.0-34, `performance`
governor set for the runs and put back to `schedutil`), `shoal-workload` at `4f3246d` against
`3d21dca`, both built for `znver1`, by the procedure in
[Benchmarking](../performance/benchmarking.md#before-and-after-on-the-lab): two shards on cpus 2
and 4, the client on cpus 6 and 7, locked memory unlimited, `/opt/shoal` on titan's root wiped
before every run, four rounds, the first side alternating. No workload authenticates, so this is
what the check costs where it can hold nobody to anything: `principals.is_empty()` a bundle.

| Workload | Figure | Before, median [range] | After | |
| --- | --- | ---: | ---: | --- |
| `transport/send_one/small` | ops/s | 48,800 [45,093–54,190] | 53,731 [50,048–53,945] | within noise, 1.10× |
| `transport/send_one/small` | get p50 µs | 300 [266–336] | 262 [251–298] | within noise |
| `get_ephemeral` | ops/s | 60,235 [58,963–61,674] | 60,494 [55,150–62,733] | within noise, 1.00× |
| `grid/unsorted/r50/1024` | ops/s | 8,364 [8,288–8,427] | 8,419 [8,364–8,541] | within noise, 1.01× |
| `grid/unsorted/r50/1024` | write p99 µs | 15,220 [13,860–16,430] | 14,471 [14,342–14,820] | within noise |
| `insert_ephemeral` | ops/s | 142,493 [99,698–144,011] | 119,007 [115,384–132,186] | within noise, 0.84× |

`insert_ephemeral`'s median came out lower on F77's side with the intervals overlapping only
through one slow run before, so it was run again for eight rounds a side: 127,832
[89,801–141,225] before and 119,300 [102,565–138,017] after, 0.93× and overlapping across most of
both ranges. It is the noisiest arm of the four on this host, as F75's runs of it were (90,671 to
144,190 on one side), and nothing in F77 runs on an unauthenticated insert but the length load.

### On the cluster, a principal held to grants

**`shoaladm bench`** on a copy of `tmdb_cluster.yaml`'s three hosts (`f77-bench`): europa (Ryzen 9
7945HX, 16 cores and 32 threads) with the driver, titan and hyperion (Zen1 V1756B), one gigabit
between them, factor three, 200,000 TMDB rows preloaded and the cluster destroyed after every run,
the `performance` governor set and put back by the bench, ten seconds of warm-up and twenty
measured, through the endpoints. Three sides, each built and deployed from its own tree: the
commit before F77 (`3d21dca`) as the admin; F77 (`4f3246d`) as the admin, whose bundles pay the
two lookups and skip the check; and F77 as `bencher`, granted `read, write` on `*`, so every query
of every bundle is checked. Four rounds, the side going first rotating. The nodes' journals show
the third side's driver authenticated as `bencher` and nothing refused.

| Arm | Before, admin: ops/s median [range] | F77, admin | F77, `bencher` | |
| --- | ---: | ---: | ---: | --- |
| `read100`, 1 | 156,085 [145,379–157,798] | 161,169 [153,915–168,778] | 153,274 [145,294–160,009] | within noise |
| `read100`, 16 | 276,854 [252,090–281,097] | 306,105 [273,988–325,398] | 274,582 [251,520–287,674] | within noise |
| `rw50`, 1 | 6,382 [6,329–6,512] | 6,542 [6,345–6,697] | 6,552 [6,346–6,604] | within noise |
| `rw50`, 16 | 47,878 [44,819–49,458] | 49,050 [44,908–49,656] | 49,634 [47,640–51,329] | within noise |

Every arm was within noise on every pair of sides, and the principal held to grants ran where the
build before F77 ran: 0.98 and 0.99 of its reads a second, 1.03 and 1.04 of its mixed. Two
latency figures came out disjoint, both in F77's favour and neither anything it could cause: the
admin side's read p99 at a bundle of one (0.500 ms [0.445–0.547] against 0.674 [0.580–0.762]
before), and `bencher`'s read p50 at sixteen (0.661 ms [0.568–0.732] against the admin side's
0.964 [0.754–1.104], which ran more reads a second in a slower regime). They are recorded as
measured, not claimed. Every side's driver met `NotLeader` between 230 and 276 times over its four
runs, as each was retried, and none `Unauthorized`.

## Tests

| Test | What breaks if F77 is reverted |
| --- | --- |
| `shoal` `authorization::a_principal_does_only_what_it_was_granted` | a reader writes, a writer reads or tests a row's existence, either reaches a table no grant names, or a refusal stops naming who, what and where; without the check all four reach the table |
| `shoal` `authorization::nothing_is_allowed_that_no_grant_names` | a principal the catalog does not name is served, or `*` stops granting a table no grant names |
| `shoal` `authorization::a_bundle_is_refused_query_by_query` | a bundle refused whole, or its refused write applied: the answers must be served, refused, refused, and the row unchanged |
| `shoal` `authorization::an_open_server_holds_nobody_to_its_grants` | a server requiring no authentication starts refusing anonymous clients because it has grants |
| `shoal` `authorization::a_grant_on_a_table_the_schema_does_not_serve_refuses_the_start` | a misspelled grant becomes a grant on nothing |
| `shoal` `authorization::a_tables_position_is_its_place_among_the_table_names` | a compiled grant indexes another table's privileges |
| `cluster_fixture` `grants_hold_on_every_node_and_change_by_committed_operations` | the seeded grant held on every node, carol refused a read, bob refused a self-grant, a grant on nothing refused, alice's grant and revoke reaching connections bob already held on every node, and the `grants` read whole to the admin and narrowed for bob |
| `shoal-core` `control::types::tests::grants_are_seeded_then_changed_by_committed_operations` | the bootstrap seed, versioning, idempotence, refusals and the map carrying the grants while the client frame does not |
| `shoal-core` `authorize::tests::grants_compile_to_a_privilege_a_table` | `*` added to a named grant, an admin holding everything, a stranger holding nothing past the last table |
| `shoal-core` `conf::tests::grants_in_the_config_load` | a table name's case lost through the config loader, `*` refused, or a privilege nobody knows read |
| `shoal-proto` `grants::tests::{privileges_parse_from_their_names, read_and_write_are_separate, a_catalog_reads_from_yaml, grants_and_revokes_change_only_what_they_name, a_grant_on_a_table_nobody_serves_is_refused}` | the catalog's own rules |
| `shoaladm` `cluster::actions::tests::a_grant_parses_previews_and_renders` | the command line's grammar, its preview and the `grants` read's lines |
| `shoaladm` `deploy::inventory::tests::users_and_grants_are_read_and_their_names_held_to_plain` | an inventory's users and grants, and a user name that would be a path |
| `shoal-bench` `deploy_render::a_rendered_node_claims_starts_and_initializes` | a rendered user or grant missing from the file the engine starts from |

**On the lab** the `tmdb` cluster was bootstrapped afresh on `4f3246d` from `tmdb_cluster.yaml`,
which since names `reader`, granted `read` on `*`. The seeded grant read back the same as the admin
and as `reader`; the admin loaded 10,000 movies; `reader`'s load was refused on its first bundle
(`reader may not write MovieByKeyword: it holds read on it`) while its `verify` read every movie,
release row and keyword partition back; its `grant reader * write` was refused as a mutation by a
principal `cluster.admins` does not name; the admin's grant of `write` let its loads through and
the admin's revoke refused them again; five grants in a row, each after a revoke, let through a
load started the moment the grant was applied, every map installed on all three nodes within the
same second; and after the cluster was stopped and started again the committed grant held. One load
straight after the first grant failed: no refusal is in any node's journal for it, it ran through
the leadership hand-back that follows a bootstrap, and it was run with `--retries 0`. The full
dataset was loaded again afterwards as the admin; one such load, started as the cluster handed its
leads back after a restart, had every row acknowledged and never finished, and is filed as
[item 225](../appendix/known-issues.md#225-a-load-that-had-every-row-acknowledged-never-finished):
it is on no path F77 changed, and two more loads under the same churn did not meet it.

## Related

[F12](authentication.md), which made the principal; [F39](membership.md), whose committed admins
and admin frame this extends; [F41](read-consistency.md), whose committed read policy it copies
the shape of; [F51](cluster-deployment.md) and [F63](shoaladm.md), the deployment that renders
users and grants; [D3](../direction/authentication.md), where authorization was first filed;
[S1](../object-storage/prerequisites.md#optional), the prerequisite it closes, and
[S2](../object-storage/buckets.md), the buckets whose grants it leaves room for.
