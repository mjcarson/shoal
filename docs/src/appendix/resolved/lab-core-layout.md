# 218. The lab's before-and-after procedure ran a node unlike the one it described

## Symptom

Since [F65](../../features/query-figures-home-tab.md), a change that adds work to a node's query
path has been checked before and after on a lab node, titan or hyperion
([Benchmarking](../../performance/benchmarking.md#before-and-after-on-the-lab)). The procedure gave
a scratch configuration of two shards with `exclude_cores: [3]`, and said the client had "one
physical core left to" it; [F73](../../features/bodies-across-frames.md)'s page named it as
"physical core 3 (cpus 3 and 7)" and pinned the client there with `taskset -c 3,7`. Neither was
true on those hosts:

- **The client shared a physical core with a shard**, and a whole core sat idle. The shards ran on
  cpus 1 and 2, the client on cpus 3 and 7, and cpu 3 is cpu 2's other thread. Cores 2 and half of 3
  ran nothing.
- **No shard registered its buffers.** Every run logged glommio's `Error: registering buffers in the
  main ring` with `ENOMEM`, so every shard wrote its WAL and its archives without `WriteFixed`. A
  deployed node does not run that way: its unit sets `LimitMEMLOCK=infinity`
  (`shoaladm/src/deploy/unit.rs`).

F65's page stated only "one physical core left to the client", and its runs were not kept, so which
cpus its client had is not known; its shards sat where F73's did. Both sides of every A/B ran under
the same layout, so no verdict taken that way is wrong as an A/B.
What each measured was a node with its client on a shard's core and its buffers unregistered,
which is not the node either page described.

## Cause

**The procedure assumed a numbering of SMT threads these hosts do not have.** On a host whose
siblings are `n` and `n + 4`, "core 3" is cpus 3 and 7. On titan and hyperion, as on most AMD
desktops, a core's two threads are adjacent: cpus 0 and 1 are core 0, 2 and 3 core 1, 4 and 5 core
2, 6 and 7 core 3 (`/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`). `exclude_cores`
filters by the physical core id the kernel reports (`Resources::cpus_reserving`,
`shoal-core/src/server/conf.rs`), and is right; the client's `taskset` was written by cpu number,
and was not. With core 3 (cpus 6 and 7) excluded and cpu 0 never a shard's, the candidates were
cpus 1 to 5, and one cpu a core in core order put the two shards on cpu 1 (core 0) and cpu 2 (core
1). `taskset -c 3,7` then put the client on cpu 3, beside the shard on cpu 2, and on cpu 7.

**The procedure runs `shoal-workload` in a login shell.** Its locked memory limit on the lab's
hosts is the default 8 MiB (`ulimit -l` is 8192), and each glommio executor asks to register 10 MiB
of buffers with its rings. The kernel refuses with `ENOMEM`, glommio warns and goes on without them
(`glommio/src/sys/uring.rs`, `register_buffers_by_ref`), and nothing in the procedure read the log.

## Evidence

**Established by reading the hosts' topology, and reproduced.** Found by
[X9](../../object-storage/table-latency.md), which needed a core given up for its own arm and
read `thread_siblings_list` on titan and hyperion before choosing one. Both hosts report adjacent
siblings. Run on titan with F73's own configuration and its `taskset -c 3,7` under a login's
limit, a reference cell's threads sat where the cause says, sampled with `ps -L -o psr` halfway
through:

| Thread | cpu | Busy |
| --- | ---: | ---: |
| Shard executor (`unnamed-1`) | 1 | 41.8% |
| Shard executor (`unnamed-2`) | 2 | 39.8% |
| Client runtime (`workload-client`), both threads | 3 | 19.3% each |
| The process's main thread | 7 | 6.6% |

Run on 2026-10-09 from `/var/tmp/x9` with `target/lab/f73/conf.yml`, its storage moved to
`/var/tmp/x9/shoal`, under `ulimit -l` 8192: both client threads were on cpu 3 at that moment, and
both shards logged `Error: registering buffers in the main ring` with `code: 12, kind:
OutOfMemory`. Under X9's corrected layout and an unlimited limit, the shards were on cpus 2 and 4,
the clients on 6 and 7, and none of X9's 208 runs logged the warning
([X9](../../object-storage/table-latency.md#where-and-on-what)).

And F73's own logs say the same of its A/B: every one of its 48 runs, both sides, logged the
registration failure, `code: 12, kind: OutOfMemory` (`target/lab/f73/out-main/*.log`, kept on
europa). F65's runs were not kept; it used the same configuration on the same host, so its shards
were where F73's were, and what its client and its buffers were is not known.

## The fix

**The procedure states the layout for these hosts, and how to check it.**
[Benchmarking](../../performance/benchmarking.md#before-and-after-on-the-lab)'s step 2 and
`CLAUDE.md`'s copy of it now give `exclude_cores: [0, 3]`, which puts the two shards on cpus 2 and
4, one a physical core, with the client on cpus 6 and 7, the whole of core 3, and core 0 left to a
coordinator that runs nothing in a standalone node. They say to read `thread_siblings_list` first,
since another host may number differently, and `ps -L -o psr` once during a run.

**Each side runs with locked memory unlimited**, as a deployed node does: the shell that runs the
sides raises its own limit first, `sudo prlimit --pid $$ --memlock=unlimited:unlimited`, and a
run's log is read for the registration warning. X9's host script does both
(`shoal-spike/results/x9-host.sh`).

F65's and F73's pages keep their tables, with the layout they stated struck and what it was put
beside it.

## Alternatives rejected

**Run F65's and F73's A/Bs again.** Each compared two builds under one layout, and both found
nothing to measure. A cost hidden by a shared core or by unregistered buffers would have had to
appear on one side only; neither change touched cores or buffers.

**Pin the client by core id.** `taskset` takes cpus, and the procedure is run by hand. Naming the
cpus and saying how to check them is what a reader can follow.

**Raise the hosts' limit in `/etc/security/limits.conf`.** That is a change to the lab's hosts for
every login, made to fix a procedure; the per-run `prlimit` changes nothing that outlives the run.

## Invariants to uphold

- **A layout is written in cpus only after the host's siblings were read.** `exclude_cores` is
  by physical core id and right on any host; a `taskset` or a `Placement::Fixed` is by cpu number,
  and right only for the numbering it was written against.
- **A lab run is a node as deployed, or says how it is not.** Locked memory is unlimited for a
  deployed node; a run without it measures shards with no registered buffers.
- **A run's log is read.** glommio reports what it gave up as a warning and goes on.

## Still open

- **The pinned figures under the new layout.** No A/B has run under the corrected layout except
  X9's, whose cell alone is its own baseline. The next change that takes the procedure gets them.
- **The memlock warning is still a warning.** A node that cannot register its buffers starts and
  serves; a deployment that forgot `LimitMEMLOCK` would run slower and say so only in its log.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| None | | A procedure has no test. X9's lab script records every cpu's core and siblings and the run's locked memory in its facts, and samples where each thread ran in its first round (`shoal-spike/results/x9-lab.sh`, `x9-host.sh`) |

## Related

[Benchmarking](../../performance/benchmarking.md#before-and-after-on-the-lab), the procedure;
[F65](../../features/query-figures-home-tab.md#performance) and
[F73](../../features/bodies-across-frames.md), the A/Bs it ran;
[X9](../../object-storage/table-latency.md), which found it;
[Thread per Core](../../architecture/thread-per-core.md).
