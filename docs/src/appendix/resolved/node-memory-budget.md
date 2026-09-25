# 149. A deployed node's memory budget was every shard's, so a node could hold its core count times it

## Symptom

During the O61 experiment on the lab, hyperion was killed by the kernel's OOM killer:
`anon-rss:14003400kB` on a 14 GB host, with the inventory's `memory: 8Gi`. Its unit's memory peak
had grown run by run, 5.5, 9.9, 10.5 and 13.3 GB, and nothing was ever evicted. Writes to its
groups failed for twenty seconds while it restarted, and no acknowledged write was lost.

## Cause

`resources.memory` in `shoal.yml` is a **shard's** budget. Each shard keeps its own
`memory_usage` counter and evicts once that counter passes `resources.memory`
(`shard.rs`, the check at the bottom of the shard loop). The configuration page says so, if
briefly: "the shard-wide budget that drives eviction".

A deployment's inventory names a **node's** memory (`Resources::memory`, "the memory limit for
every node"), and the renderer wrote it unchanged as `resources.memory`. With six cores the
lab's 8 GiB nodes had six 8 GiB budgets: 48 GiB of table data before any shard evicted, on hosts
with 14 GB. The whole dataset fitted, so nothing was ever evicted, and a node's resident memory
grew with every row it loaded until the kernel ended it.

## Evidence

**Found on the lab.** hyperion's kernel log at 16:40:35:

```text
oom-kill:…,task_memcg=/system.slice/shoal-tmdb.service,task=tmdb-dataset-no,pid=361868,uid=101
Out of memory: Killed process 361868 (tmdb-dataset-no) total-vm:24044628kB, anon-rss:14003400kB
```

Its journal has no eviction pass at all over the half hour before it. The rendered `shoal.yml`
on every node says `memory: 8Gi` under `resources: {cores: 6}`.

**Dividing the budget was not enough, and the lab showed why.** With `node_memory` on every node,
five minutes of inserts still took europa from 4.4 to 9.3 GB and titan from 8.1 to 12.4 GB, and not
one eviction pass ran. With the node budget cut to 2 GiB on europa to force eviction, the eviction
lines gave the shards' own counters: 6 to 17 MB of rows each, about 100 MB for the node, in a
process holding 4.3 GB. The counter covers rows, as estimated by `deepsize`, and almost nothing a
node holds is rows: the archive map's entry per partition, the table maps' buckets (#150), the
WAL's index and caches, and allocator pages. A budget judged by that counter could not bound a
node.

**Tested** by `a_nodes_memory_is_shared_among_its_shards` (`shoal-core`, `conf.rs`),
`a_nodes_resident_memory_is_read_from_statm` (`shoal-core`, `shard.rs`) and
`a_node_file_names_its_memory_as_the_nodes_budget` (`shoalctl`), written with the fix. The defect
was established from the lab and the source, not by a test run against the unfixed tree. The lab
runs before and after are the measurement: see the table below.

## The fix

- **`resources.node_memory`**, optional, is the most the node holds, judged against **the
  process's resident memory**. Every shard reads `/proc/self/statm` at most every 250 ms
  (`MEMORY_CHECK`), and a process past the budget has each shard evict its usual 40% once per read
  (`Shard::over_memory`). The shard's counter still bounds its rows at `memory`, or its share of
  `node_memory` (`Resources::shard_budget`), whichever is less. `memory` keeps its meaning, so every
  existing configuration, the benchmark's `shoal.yml` included, evicts exactly as before.
- **The renderer writes the inventory's memory as `node_memory`** as well as `memory`.

| Run on the lab, five minutes of 70% inserts | europa | titan | hyperion |
| --- | --- | --- | --- |
| `memory: 8Gi` alone, every shard's | grew to 12.8 GB | 12.1 GB, then OOM-killed | 13.2 GB, OOM-killed |
| `node_memory: 8Gi` divided among shards, counter only | 4.4 → 9.3 GB, still growing | 8.1 → 12.4 GB | 4.3 → 9.1 GB |
| `node_memory: 8Gi` judged by resident memory | not measured: still on an earlier build* | (rebuilding, #151) | 7.8–8.4 GB, level |

\* europa had been restarted for the 2 GiB experiment below and was not restarted again, so
through this run it held a 2 GiB budget on the reverted build that counted the archive map. Its
figures, 8.3–9.3 GB and 149,825 eviction passes, describe that thrash and not this fix. A run with
every node on this build is in [cluster testing](../../cluster-testing/performance.md#memory).

Resident memory now sits at the budget and not at the host's limit. The kernel's figure includes
pages the allocator has not yet returned, so it overshoots the budget by that much. Leave the
headroom for it: the lab's 8 GiB on 14 GB hosts does.

## Alternatives rejected

- **Count the archive map in the shard's counter.** Tried and reverted. A shard's archive map
  alone was 400 MB against a 341 MB share of a 2 GiB node, so every pass of the loop was over
  budget and evicted the one partition it could: 45,995 eviction passes in two minutes, and no
  cache. Every structure outside the counter would have needed the same, and each estimate would
  have drifted like the first. The kernel's figure already counts them all.

- **Divide in the renderer.** A node whose inventory leaves `cores` unset runs a shard on every
  core the control plane leaves, which the renderer cannot know. The server knows at start.
- **Make `resources.memory` a node's budget.** It would change what every existing
  configuration means, including the benchmark's `shoal.yml`, whose captures are compared across
  commits. A new key keeps the old meaning where it is written and gives deployments the one they
  meant.
- **One node-wide counter shared by every shard.** Thread per core means nothing a shard does
  on its hot path is shared. A shard's share of the node is the same bound without the sharing.

## Invariants to uphold

- **A deployed node's table data is bounded by the node's memory**: the renderer writes
  `node_memory`, and `shard_budget` divides it by the shards the node runs.
- **`node_memory` never raises a shard above `memory`.**
- **A node budget is judged against what the kernel says the process holds**, never an estimate.
  A shard's counter bounds its rows and nothing else.
- **The read is rate-limited.** Past its budget with nothing evictable, a shard evicts once per
  interval, not on every message (the shape of [known issue 59](../known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence)).

## Still open

- **A node whose structures outside the rows exceed its budget evicts every row, four times a
  second, and stays over.** The archive map grows with every partition ever written (#150's
  *Still open*). Filed in [todos](../todos.md), with a node's memory figures on `Stats`.
- **A deployment's `shoal.yml` is written once.** `cluster upgrade` swaps the program and never
  re-renders the file, so the lab's nodes had `node_memory` added by hand. Filed in
  [todos](../todos.md).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_nodes_memory_is_shared_among_its_shards` (`shoal-core`, `conf.rs`) | A node budget is not divided among shards, or lifts a shard past `memory`, or does not parse |
| `a_node_file_names_its_memory_as_the_nodes_budget` (`shoalctl`, `render.rs`) | A deployed node's memory is written as every shard's budget again |
| `a_nodes_resident_memory_is_read_from_statm` (`shoal-core`, `shard.rs`) | The node's budget is not judged by the process's resident memory |
| The memory runs on the lab ([findings](../../cluster-testing/findings.md)) | A lab node grows past its host's memory under a long insert load and is killed |

## Related

- [Memory and eviction](../../tables/memory-and-eviction.md), the per-shard counter.
- [Known issue 59](../known-issues.md#59-a-shard-that-cannot-free-anything-keeps-trying-on-every-message-in-silence),
  why a shard over its budget is silent.
- [F51](../../features/cluster-deployment.md), the inventory and its renderer.
