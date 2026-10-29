# 255. A restore refused a backup cut under another schema, so a schema change had no path

A restore held a backup to the schema id it was cut under, and every change to a schema moves that
id, so the path the docs gave for a schema change, a new cluster and a restore, was refused for
every change there is. [Q31](../../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)
decided the fix by design, and [F93](../../features/operations-and-the-real-cluster.md) built it at
M21: a restore is judged table by table, and an addition is made rolling, so it needs no restore at
all.

## Symptom

On the lab, in Q31's run (E3a), a backup of the tmdb cluster restored into a fresh cluster of its
schema with one table appended was refused:

```text
the backup was cut from schema 0xadb279928c5ccf2a and this cluster serves 0x8ccf0556985b6bc9;
a schema change is not a restore
```

under the code `Internal`, as though the server had failed. A backup restored only into a cluster
of the very schema that cut it, which is recovery from a lost cluster and no migration.

## Cause

The control plane's `AdminKind::Restore` arm compared `source_schema` with the member's schema id
before it proposed anything. A schema's id is the derive's hash over every field, so a table added,
a field renamed or a bucket declared moved it, and the comparison refused them all alike. The
snapshot files a backup holds carried the schema id in their header and manifest too, so the
comparison stood for every table at once where only the tables that changed differ.

## Evidence

**Reproduced on the lab** in Q31's run (E3a), recorded in the
[contract's record](../../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17),
the refusal above. The fixture's `a_restore_judges_each_table_by_its_own_identity` was written with
the fix: a backup of `ObjectDb` restores into `v2`, which appends a table and a bucket, and is
refused by `v3`, whose document's row changed.

## The fix

- **Each table has an identity of its own**, `table_fingerprint`: its name, its type's spelling, its
  row's fingerprint and its projections, with the object rows' layout for a bucket's generated
  tables. It replaces the schema id in a snapshot's header and manifest, a backup's manifest, the
  install's check, the scrub digest and an export.
- **A restore judges each file** by `judge_table`: a bucket's generated table is refused by name, a
  table served at the same fingerprint is restored, one served at another is refused naming the
  table and both shapes, and one not served is refused. Nothing compares the schemas' ids, and a
  source group's placement state is never carried.
- **A refusal is named**, `InvalidRequest` with its reason, never `Internal`.

## Alternatives rejected

**A schema id compatibility table.** Recording which ids may restore into which is a list somebody
keeps; a table's own identity says it without one.

**Restoring a changed table by converting its rows.** That is an import, with a mapping only the
operator can write; a restore that guessed one would restore something nobody backed up.

## Invariants to uphold

- **A table's fingerprint is the only identity a file carries.** A schema id in any header or
  manifest a restore or an install reads brings this back for every append.
- **A bucket's rows never restore as table files**, whichever schema cut them: a bucket is backed up
  and restored as its objects.

## Still open

A change other than an append, a table removed, renamed or changed, is still a new cluster and a
restore that refuses the changed table; an import is
[todo'd](../todos.md#what-q31-left-for-later).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::a_restore_judges_each_table_by_its_own_identity` | A backup cut before an addition is refused, or one whose row changed is restored |
| `cluster_fixture::rows_older_than_chunks_are_refused_by_name` | A backup holding a bucket's rows as table files is restored |
| `shoal-core control::schema::tests::a_file_is_judged_by_its_own_table` | A table is judged by the schema's id, or a generated table passes |

## Related

[F93](../../features/operations-and-the-real-cluster.md), [F49](../../features/backup-and-recovery.md),
[Q31's record](../../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17).
