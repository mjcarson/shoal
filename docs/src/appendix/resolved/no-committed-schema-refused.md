# 270. A cluster upgraded in place from wire 14 ran with no committed schema

Since [F93](../../features/operations-and-the-real-cluster.md) a cluster's schema is committed in
its control state, by `Initialize` and then by every `ExtendSchema`, and wire 15 keeps no older
reader: a cluster on wire 14 is bootstrapped again. Nothing held a node to that. A control log whose
`Initialize` came before F93 reads its schema as none, and only `ExtendSchema` sets one, which is
refused without one, so an F93 build started on such a log ran on with no committed schema for
good. Filed by a code review of F87 to F93.

## Symptom

Such a cluster could never extend its schema (`shoaladm upgrade --activate` proposes none when the
committed schema is unknown), refused every table of a restore as one it does not serve, and held no
start to a schema: a build below the cluster's, which the committed schema is there to stop, started.
Nothing said why.

## Cause

`apply_restore` judges each file against `self.schema.unwrap_or_default()`, `apply_extend_schema`
refuses `NotInitialized` without one, and the start's check of its own log stops only a build below
a committed schema, so a log that committed none passed every check that reads it.

## Evidence

**Reproduced** by `a_start_on_a_log_with_no_committed_schema_is_refused`, written first and run on the
unfixed judgment: the start's two checks of its own log moved unchanged into
`ControlState::judge_start`, against a state initialized with no schema.

```text
thread 'server::control::types::tests::a_start_on_a_log_with_no_committed_schema_is_refused'
panicked at shoal-core/src/server/control/types.rs:5725:9:
Ok(())
```

## The fix

**A node refuses to start, by name, on a log that says the cluster was initialized and names no
committed schema** (`ShoalError::SchemaNotCommitted`): "holds the log of a cluster initialized with
no committed schema, as a cluster before wire 15 was; such a cluster is bootstrapped again and
restored, never upgraded in place". The start's checks of its own log are one method,
`ControlState::judge_start`. An upgrade onto F93 of such a cluster now stops at its first node,
whose restart fails and is reverted onto the build it ran, which is where the operator is told.

## Alternatives rejected

- **An `ExtendSchema` from none that adopts a schema whose tables are the committed tables.** It
  would let such a cluster carry on, against F93's decision that wire 14 is bootstrapped again: the
  wire keeps no older reader, so the cluster could not be read by what it upgraded from either way.
- **Refusing `activate 15` instead.** The cluster would run on the new build below the activation,
  with the schema's checks still skipped; a start is the first point a node can tell.

## Invariants to uphold

- **An initialized cluster has a committed schema.** `Initialize` commits one from a non-empty
  lineage, and a start refuses a log that holds none.

## Still open

Nothing.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `control::types::tests::a_start_on_a_log_with_no_committed_schema_is_refused` | A node starts on a log initialized with no committed schema |

## Related

[F93](../../features/operations-and-the-real-cluster.md), which committed the schema;
[F48](../../features/rolling-compatibility.md) for the start's check of its activated wire version.
