# 201. An attached run that read before it inserted was refused, though the run loads the preload first

Filed and fixed in one change, from a user's first run through the bench wizard
([F67](../../features/bench-run-wizard.md)) against an attached cluster.

## Symptom

The user ticked an insert workload in the wizard and allowed writes. The review page still held
one error and would not start the run:

```text
a read of an attached cluster needs the preload in it: pass --preloaded if it is, or put a
workload that inserts first
```

Nothing in the wizard could fix it:
- **"put a workload that inserts first"**: the wizard lists `read100` ahead of `insert100`, and has
  no way to reorder them;
- **`--preloaded`**: it says the cluster already holds the preload, which was not true.

The same run from the command line, `--attach --workloads read100,insert100 --yes-write`, was
refused the same way, and so was the default set of workloads against any attached cluster.

## Cause

[F66](../../features/dataset-benchmarks.md)'s attach rules (`BenchRunArgs::problems`) said:
**a read needs `--preloaded` or an insert arm before it**. The rule was implemented as "the first
workload that writes is listed before the first that reads".

That is not what the run does. An attached run that is not `--preloaded` **loads the preload
itself, before its first arm** (`orchestrate`, the `!loaded` branch), and reads ask only for
preloaded keys. So every read finds its row whatever order the workloads are listed in. The
order was not even the order the arms run in: `BenchSpec::arms` starts each run one arm later,
so the second run's first arm is not the first workload.

What does need permission is the preload's **writes**, and the rule never checked those. A read
only run with neither flag was refused for the wrong reason, and a run that inserted was
refused, or not, by an order nothing used.

## Evidence

**Reproduced.** Three tests written before the fix fail on the unfixed tree with the user's
refusal:

```text
test bench::args::tests::an_attached_read_first_runs_once_writes_are_allowed ... FAILED
["a read of an attached cluster needs the preload in it: pass --preloaded if it is, or put a workload that inserts first"]
test bench::wizard::form::tests::an_attached_run_that_reads_first_starts_once_writes_are_allowed ... FAILED
[Issue { severity: Error, page: Reads, row: None, message: "a read of an attached cluster needs the preload in it: ..." }]
test a_run_naming_no_workload_runs_the_defaults ... FAILED
the run finishes: the bench cannot run:
  - a read of an attached cluster needs the preload in it: pass --preloaded if it is, or put a workload that inserts first
```

On the fixed tree, the last of these runs the four defaults against a fresh node by `--addr`, with
`read100` first. The read arm found every row it asked for (`read.misses == 0`), because the run
loaded the preload before it.

## The fix

**An attached run that is not `--preloaded` needs `--yes-write`, whatever its workloads are.**
The run writes in two ways, and the check now names both:
- a workload that inserts, as before;
- the preload it loads before its first arm, unless the cluster already holds it.

The order check is gone. In the wizard, the refusal lands on the Workloads page, beside the
`--yes-write` row that fixes it. That row's help, and `read100`'s, now say that the preload is a
write.

## Alternatives rejected

- **Ordering the wizard's workloads inserts first.** That would satisfy the rule without making
  it true, and leave the command line refusing a run that works.
- **Letting the wizard reorder workloads.** Same objection: it teaches an operator a constraint
  that does not exist.
- **Not writing the preload on an attached cluster at all.** A run with only an insert workload
  then has no read keys, and every later read arm misses. The preload is what reads ask for.

## Invariants to uphold

- **Every write the bench makes to an attached cluster needs `--yes-write`**: inserts and the
  preload alike. `--preloaded` is the only way to read an attached cluster without writing to it.
- **The preload is loaded before the first arm of an attached run that is not `--preloaded`.** If
  that ever moves, a read arm that runs before an insert arm needs a rule again, and the rule
  must follow `BenchSpec::arms`' rotation, not the list order.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoaladm` `bench::args::tests::an_attached_read_first_runs_once_writes_are_allowed` | Reads listed before inserts are refused with writes allowed, or a read only run with neither flag is not asked for one |
| `shoaladm` `bench::wizard::form::tests::an_attached_run_that_reads_first_starts_once_writes_are_allowed` | The wizard's defaults against an attached cluster cannot start once writes are allowed |
| `bench-dataset` `bench_run::a_run_naming_no_workload_runs_the_defaults` | The defaults are refused against a node by `--addr`, or the read arm that runs first misses rows |

## Related

- [F66](../../features/dataset-benchmarks.md), whose attach rule this corrects.
- [F67](../../features/bench-run-wizard.md), whose wizard found it.
