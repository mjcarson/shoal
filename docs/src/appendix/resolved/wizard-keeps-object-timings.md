# 344. An inventory edited in the wizard lost its object timings

## Symptom

An inventory edited with `shoaladm new --from <inventory>` was written back without its
`object_timings`. A deployment that shortened its reclamation's grace (`reclaim_grace: 30s`, as the
lab's runs do) and was edited in the wizard for anything else went back to the engine's five
minutes at its next `reconfigure`, with nothing said.

## Cause

The wizard loads an inventory into a draft (`Draft::from_inventory`,
`shoaladm/src/wizard/form.rs`) and builds a new inventory from the draft when it saves
(`Draft::build`). Every setting the form has no field for is carried in the draft as it was, and
put back by `build`: the replication block, the WAL commit delay, plaintext lanes, the congestion
control, the users and grants among them. `object_timings`, added with
[F92](../../features/reclamation-and-device-lifecycle.md), was not carried: `from_inventory` did not
read it and `build` wrote an empty map.

## Evidence

Established by a test written first, found while milestone 4 of the scans plan carried an
inventory's `scans:` through the wizard (2026-10-30). The wizard's round-trip row,
`a_draft_round_trips_an_inventory`, loads an inventory into a draft, builds it again and compares
the two; given `object_timings: {reclaim_grace: 30s, reclaim_poll: 10s}`, against the unfixed tree
the built inventory's field was empty:

```text
thread 'wizard::form::tests::a_draft_round_trips_an_inventory' panicked at
shoaladm/src/wizard/form.rs:2354:13:
  left: ... object_timings: {} ...
 right: ... object_timings: {"reclaim_grace": String("30s"), "reclaim_poll": String("10s")} ...
```

Not met on the lab, whose inventories are edited by hand.

## The fix

The draft carries `object_timings` as it carries `scans`: read by `from_inventory`, put back by
`build`, empty in a new draft. The form still has no field for either.

## Alternatives rejected

- **A field for the timings in the form.** A free map of keys only the engine judges, which the form
  could only take as text; the round trip is what an edit needs.
- **Keep every key the form does not know by re-reading the source file at save.** A second parse
  and a merge of two inventories, where one draft field a setting is what the form already does.

## Invariants to uphold

- **Every inventory field the form has no editor for is carried by the draft unchanged**: read in
  `from_inventory` and written in `build`. A new inventory field joins
  `a_draft_round_trips_an_inventory`'s inventories in the change that adds it.

## Still open

Nothing of this item.

## Tests

| Test | Where | Breaks if |
| --- | --- | --- |
| `a_draft_round_trips_an_inventory` | `shoaladm/src/wizard/form.rs` | An edit drops an inventory's object timings or its scan settings, or any other field the form has no editor for |

## Related

[F53](../../features/inventory-wizard.md), the wizard; [F92](../../features/reclamation-and-device-lifecycle.md)
for the timings; [the shoaladm page](../../operations/shoaladm.md#scan-settings) for the scan settings
carried beside them.
