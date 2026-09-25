# 127. The wizard saved an inventory bootstrap refused from the same host

## Symptom

An inventory written by `cluster new` ([F53](../../features/inventory-wizard.md)) with every
node's address left blank saved with no issue, and `bootstrap` then refused it on the host it was
written on:

```text
     Running `target/debug/examples/tmdbctl cluster bootstrap -i tmdb_cluster.yaml`
Error: europa resolves only to loopback addresses; give it an address in the inventory

Location:
    shoalctl/src/deploy/inventory.rs:811:13
```

On europa, `getent hosts europa` answers `127.0.1.1` from `/etc/hosts`, while titan and hyperion
answer their lab addresses. The same inventory also named `shoalctl/examples/tmdb/tables.rs`, a
source file, as its `server`. Both the wizard and `bootstrap` accepted that, and `bootstrap` would
have copied the source file to every host as the node program.

## Cause

The wizard says it judges the draft "the way `bootstrap` will". It did so for everything
`Inventory::validate_shape` checks, but a blank address is not a shape. `bootstrap` resolves it in
`Inventory::node`, through `resolve`, which refuses a name that resolves only to loopback. The
wizard never resolved a name, so the one refusal it could predict was the one it did not make. The
address field's help text even said "a loopback answer is refused", without anything to back it.

`validate` checked only that `server` was a file (`is_file`), and the wizard only warned when it
was not one. A source file passes both checks.

## Evidence

**Reproduced against the unfixed tree.** The report above is the user's. Two tests were written
first and failed before the fix:

```text
---- wizard::form::tests::a_loopback_only_name_is_refused_before_saving stdout ----
the wizard would save it: []

---- deploy::inventory::tests::an_inventory_that_cannot_be_a_cluster_is_refused stdout ----
called `Result::unwrap_err()` on an `Ok` value: ()
```

The first builds a draft whose one node is `localhost`, which resolves only to loopback on every
host. `Inventory::bootstrap_nodes` refuses the built inventory, and the wizard reported no issue at
all. The second hands `validate` an ordinary temp file as the server program, and validation
passed.

## The fix

**The wizard resolves every blank address's name, off the event loop, and judges the answer.**

- `inventory::resolve` is split into `lookup`, which returns every address including loopback, and
  `dialable`, which picks the address a peer can dial. `resolve` is the two together and keeps its
  message, so deploy is unchanged. `probe::resolution` maps the pair to a `Resolution`: `Resolved`,
  `Loopback`, or `Failed`.
- `Wizard::resolutions` caches the answer by node name. On each pass, `event_loop` asks
  `Wizard::unresolved` for the names not yet looked up, marks them `Running`, and resolves each on
  a blocking thread. The answer comes back on a channel, the same way a probe's does.
- `Wizard::build` adds the issues after `Draft::build`:
  - `Loopback` is an **error** on the node's address field, which blocks saving.
  - `Failed` is a **warning**.
  - `Running` and `Resolved` add nothing.
- The Nodes page shows `10.0.0.1 (resolved)`, `resolving…`, `loopback` or `unresolved` where it
  used to show the word `resolve`.

**A server program has to be executable.** `inventory::is_executable` checks for any execute bit
(unix only). `validate` refuses a file without one with
`the server program … is not executable; name the built node program, not its source`, and the
wizard warns with the same words.

## Alternatives rejected

- **Resolving inside `Draft::build`.** `build` runs on every key. A name the resolver does not know
  can take seconds to fail, and every keystroke would wait for it. It would also make the draft's
  pure tests depend on the machine's resolver.
- **Writing the resolved address into the file.** That would freeze whatever the name resolved to
  today, possibly a DHCP lease, without the operator ever typing it. The wizard shows the address
  and leaves the choice to the operator.
- **Refusing a name that does not resolve at all.** An inventory is often written before its
  hosts exist, or on a laptop off the lab's network. That is the same reasoning that keeps an
  unbuilt server program a warning. Loopback is different: it is a definite answer, and bootstrap
  refuses it.
- **Accepting a loopback answer in `resolve` and advertising it.** A node that advertises
  `127.0.1.1` is unreachable from every other host, which is why `resolve` refuses it in the first
  place.
- **Checking the server program for an ELF header.** That is stricter than bootstrap needs and
  wrong for a script wrapper. An execute bit is what `install` preserves and what a host needs.

## Invariants to uphold

- **The wizard and bootstrap resolve through the same functions.** `probe::resolution` is `lookup`
  plus `dialable`, and `resolve` is those two as well. If either one changes its rule (for example,
  preferring IPv6), the wizard follows automatically. A second copy of the loopback rule would let
  the two drift apart again.
- **Nothing on the event loop resolves a name.** Every lookup runs through `spawn_blocking` from
  `spawn_resolutions`. A lookup called from `build`, the view or a key handler would freeze the
  screen for as long as the resolver takes.
- **A name is looked up once per session.** It is marked `Running` before its thread starts, so
  the next pass does not ask again. The cache is keyed by name, so renaming a node looks up the new
  name and the old entry is simply unused.
- **The answer is only as good as the machine the wizard runs on.** Bootstrap resolves on whatever
  host runs it. That is normally the same host, and it is the case this item was about.

## Still open

- An inventory written on one host and bootstrapped from another is judged by the first host's
  resolver. The wizard cannot know where bootstrap will run.
- The cache is never refreshed. A host whose DNS record is fixed while the wizard is open still
  shows its old answer until the wizard is restarted.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_loopback_only_name_is_refused_before_saving` (`shoalctl/src/wizard/form.rs`) | A draft whose only node is `localhost` builds with no issue and saves, although `bootstrap_nodes` refuses it. It also pins `Failed` as a warning, and `Resolved` or a given address as no issue. |
| `a_source_file_is_not_a_server_program` (`shoalctl/src/wizard/form.rs`) | A server path that is a plain file raises no warning. |
| `a_resolution_tells_loopback_from_nothing` (`shoalctl/src/wizard/probe.rs`) | `localhost` is not `Loopback`, or a name under `.invalid` is not `Failed`. |
| `an_inventory_that_cannot_be_a_cluster_is_refused` (`shoalctl/src/deploy/inventory.rs`) | `validate` accepts a non-executable file as the server program. |
| `a_node_resolves_off_the_loopback` (`shoalctl/src/deploy/inventory.rs`) | `lookup` drops loopback, so the wizard cannot tell `Loopback` from `Failed`; or `dialable` stops preferring a routable IPv4 address. |

## Related

- [F53](../../features/inventory-wizard.md), the wizard.
- [F51](../../features/cluster-deployment.md), which introduced `resolve` and its loopback refusal.
- `shoalctl/inventories/lab.yml`, which gives europa's address by hand for this exact reason.
