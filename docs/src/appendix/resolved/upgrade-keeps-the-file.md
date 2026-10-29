# 267, 268. An upgrade's carried file was written apart from the program it was carried for

Since [F93](../../features/operations-and-the-real-cluster.md) `shoaladm upgrade` writes the
inventory's pools and bindings into each node's `shoal.yml`, since a build that appends a bucket
starts only with the bucket bound, and the build before it refuses a binding of a bucket its schema
does not declare. The program was kept as `<program>.prev` and swapped by a rollback and a revert;
the file was written beside that, after the program had moved, and was kept nowhere. So a rollback
took the program back and left the file (item 267), and a failure to carry the file came after the
program had moved and undid nothing (item 268). Both were filed by a code review of F87 to F93 and
share one cause: the file and the program were two installs where they are one.

## Symptom

- **Item 267.** After an upgrade to a build that appends a bucket, before `--activate`,
  `upgrade --rollback` brought every joiner back, and refused on the bootstrapper, the one node that
  checks its bindings at a start: "storage.buckets binds Stills, which the schema does not declare".
  The wait failed, the revert swapped the new program in again, which started, and the rollback
  stopped there with the nodes before the bootstrapper rolled back and the rest not.
- **Item 268.** A failure of any step of the carry - the file's read, its YAML, a file with no
  `storage` block, bindings that do not parse, the owner's `stat`, the write - returned through `?`
  after the new program had been moved over the old one, with no restart and no revert. The node
  ran on its old process over the new program, which its next restart started without the
  bindings, and a rerun found its program already the new one and skipped it. On the failure path
  the old file's write back was a `?` too, which skipped the revert when it failed.

## Cause

`upgrade_node` moved the program (`cp -p <program> <program>.prev; mv -f <candidate> <program>`)
and only then called `carry_object_policy`, which read, judged and wrote the file in place. The
file's previous contents were held in memory for the upgrade's own failure path and nowhere on
the host, so `swap`, which a rollback and a revert both run, had only the program to swap.

## Evidence

**Item 267 is reproduced** by `a_swap_takes_the_program_and_its_file_back_together`, written first
against the swap as it was, moved unchanged into a script builder (`swap_script`) and run locally
under a `sudo` that runs what it is given:

```text
thread 'deploy::upgrade::tests::a_swap_takes_the_program_and_its_file_back_together' panicked at
shoaladm/src/deploy/upgrade.rs:1233:9:
assertion `left == right` failed
  left: ("new file", "old file")
 right: ("old file", "new file")
```

**Item 268 is established by reading the source**, twice: by the code review and by a second
reading, which found that a rerun skips the node and that the failure path's write skipped the
revert. Its order is held by the install script's test now, but no test drives `upgrade_node`,
which runs over ssh; the lab's deploy smoke test is the one that does.

## The fix

- **The file is carried before the program moves.** `carry_object_policy` reads and judges the
  node's file and writes the carried one beside it as `<file>.candidate`; a failure of any of it
  removes both candidates and stops with the node as it was.
- **The program and the file are installed as a pair** (`install_script`): the program and the
  file it runs on are kept as `<program>.prev` and `<file>.prev`, unless the node is given the
  program it runs, and the candidates are moved in together, in one script.
- **A swap takes the pair** (`swap_script`): a rollback and a revert swap the file with
  `<file>.prev` beside the program, where one was kept; a node upgraded before keeps its program
  swapped alone.
- **A file put back beside a program it already ran is best effort**, before the revert, and never
  skips it.

## Alternatives rejected

- **A rollback that rewrites the bindings for the old build.** shoaladm cannot read which buckets a
  kept program's schema declares; the file it ran on is the one answer that is right for it.
- **Writing the file in the same command as the program without a candidate.** The read, the
  judging and the write are what fail, and each needs the host's answer; a candidate is how they
  all happen before anything moves.
- **Keeping only the pools and bindings the upgrade changed.** A `.prev` of the whole file is the
  file the program ran on, which needs no merge.

## Invariants to uphold

- **A program and the file it runs on are kept, installed and swapped as a pair.** A step that
  writes one writes the other beside it, or a rollback takes a program back onto a file it was
  never given.
- **Nothing the node runs changes until every read and write the upgrade needs has been made.**
  The candidates are the only files written up to the install script.
- **A failure after the move goes to the revert.** No `?` stands between the install and it.

## Still open

- **A `reconfigure` since an upgrade is undone by its rollback**, since `<file>.prev` is the file
  the old program ran on and the rollback puts it back. A `reconfigure` after a rollback renders
  the inventory's bindings again, which the bootstrapper of the old build refuses if the inventory
  binds a bucket it does not declare.
- **No test drives `upgrade_node` against a host.** The deploy smoke test does, on the lab, and was
  not run for this change.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoaladm deploy::upgrade::tests::a_swap_takes_the_program_and_its_file_back_together` | A rollback swaps the program and leaves its file |
| `shoaladm deploy::upgrade::tests::an_upgrade_and_its_rollback_move_the_program_and_its_file_together` | The upgrade keeps no file beside the program, or moves the carried file apart from it |

## Related

[F55](../../features/cluster-upgrade.md), the upgrade; [F93](../../features/operations-and-the-real-cluster.md),
which carries the bindings.
