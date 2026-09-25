# 157. `cluster destroy` failed on a storage path that is a mount point

## Symptom

After the disk-full test, `cluster destroy` stopped at hyperion, whose storage path was a loop
filesystem's mount point, and left the deployment half destroyed.

## Cause

`destroy`, and `bootstrap --wipe`, removed each node's storage roots with `rm -rf <root>`. On a
mount point `rm -rf` empties the directory and then fails to remove it (`Device or resource
busy`). The command's non-zero status failed the step.

## Evidence

**Found on the lab** ([cluster testing](../../cluster-testing/correctness.md#fill-a-nodes-disk)).
**Tested** by `a_root_that_cannot_be_removed_is_emptied` (`shoalctl`, `deploy/ops.rs`), with a root
whose parent is read-only standing in for a mount point, since a unit test cannot mount.

## The fix

`remove_roots` builds the command per root: `rm -rf`, and where that fails, delete everything under
the root (`find <root> -mindepth 1 -delete`) and leave the root for its mount.

## Alternatives rejected

- **Always empty and never remove the root.** It would leave an empty directory behind for every
  destroyed node on hosts where the root is an ordinary directory.

## Invariants to uphold

- **A root that cannot be removed is emptied, and the command succeeds.**

## Still open

- Nothing.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_root_that_cannot_be_removed_is_emptied` (`shoalctl`, `deploy/ops.rs`) | A root that cannot be removed fails `destroy` and `--wipe` |

## Related

- [F51](../../features/cluster-deployment.md), the deployment tool.
